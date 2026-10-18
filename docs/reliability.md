# HTTP and realtime reliability

`Middleware.Protect` applies recovery, admission limits, authentication,
credential quotas and request validation. Put optional CORS and logging outside
it. Do not also apply the individual authentication and rate middleware.

## Authentication and quotas

The minimal `/api/v1/health/live` and `/api/v1/health/ready` probes and the root
service index are public for GET/HEAD. Health detail uses the ordinary
authentication policy. Fixed keys use
`X-API-Key` or `Authorization: Bearer`; query keys remain unsupported. A host
can inject validation for its temporary pairing keys.

Without a valid key, local access requires a loopback connection, loopback Host,
an absent or same-origin Origin, and no Forwarded, X-Forwarded-For or X-Real-IP
headers. CORS does not grant write access. A proxy which removes all forwarding
information and rewrites Host is indistinguishable from a local process; public
reverse proxies must require configured keys. Forwarding headers never select
the rate-limit identity or the logged peer IP.

Authentication attempts share an independent per-peer admission bucket, sized
at `max(60, 10 * RateLimit)` per minute. Authenticated business requests share a
quota per SHA-256 credential identity; callers using the same key share that
quota. Public probes have separate IP buckets and cannot consume the business
quota. Rejections include `Retry-After: 60`.

## Requests and errors

JSON bodies default to 2 MiB. Upload and Skill multipart endpoints retain their
separate handler limits, with a 64 MiB transport ceiling. The transport checks
the full body before invoking an operation, including unknown-length requests.
`DecodeJSON` also supports direct handler use and rejects trailing JSON values.
An omitted Content-Type remains compatible with native clients; an explicit
non-JSON type is rejected for JSON operations.

Oversized bodies return 413, unsupported media types 415, and malformed or
multiple JSON values 400. Errors preserve `error`, numeric HTTP `code` and
optional `details`, and add `error_code` plus `request_id`. `Protect` generates
an `X-Request-ID` response header. Neither submitted keys nor bodies are echoed
by request validation errors.

## Atomic run acceptance

Use `RunStore.CreateRun`, not `UpsertRun`, for request admission. Creation is
atomic for one store instance and durable before `created=true` is returned.
The identity is `(session_id, id)`; input comparison includes message, stream,
iteration limit, profile and attachments. An identical retry returns the existing
record without resetting state. Different input returns `ErrRunConflict`.

`GetRun` returns an independent copy. `UpsertRun` remains available for existing
administrative callers. One runtime owns the store directory; concurrent
processes writing the same directory are not a supported deployment model.
This acceptance contract does not make external tool side effects transactional
across a process crash. Hosts still own task checkpoint and execution recovery.

## Replay and connection shutdown

The host calls `Client.BeginReplay` before taking a durable event snapshot, sends
it using `SendReliable`, then calls `EndReplay` with delivered event IDs. Live
broadcasts are buffered during replay and flushed afterward, skipping snapshot
duplicates. The live buffer is bounded at 256 events. A full buffer, send timeout
or close must terminate the connection so it can retry from its received cursor;
the host must not silently continue after a failed send.

`SendReliable` can be interrupted by its context, `Client.Close` or `Hub.Stop`.
`SendToSession` also observes shutdown. Legacy `TrySend` stays nonblocking and
returns false on a full or closed queue. `ReplayWithError` and
`ReplayForReconnectWithError` expose damaged-storage failures; older methods
retain their signatures. Hosts needing reliable recovery should use the methods
which return errors.

## Incremental persistence

Each session has its own lock and newline-delimited `.jsonl` journal. Mutations
are synced before being accepted. Periodic atomic `.json` snapshots include the
last journal sequence, so a crash between snapshot replacement and journal
rotation cannot duplicate events. Checkpoint intervals grow with snapshot size,
avoiding a complete session rewrite for every stream chunk.

Existing JSON snapshots remain readable. A truncated final journal record can
be removed before appending; corruption in the middle is rejected and is never
silently overwritten. Keep both the snapshot and journal when backing up or
moving a runtime. Older binaries do not read journal mutations since the last
snapshot; rollback requires a compatible reader or an explicit export.

The seven-day retention rule remains; queued/running work is retained. Cleanup
is performed on access and checkpoint rather than by a background disk sweep.
Active history is retained for replay and is not a bound on total memory use.
On Unix, directory entries are also synced. Windows retains file sync and
rename but skips unsupported directory sync; it has a weaker directory-entry
guarantee on sudden power loss. Windows builds are cross-compiled in validation;
that is not a Windows runtime test.
