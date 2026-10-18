# WebSocket run recovery

`websocket.RunStore.Restoreable` returns queued and running runs for the host to
resume. The host still owns execution and startup. This adds no route,
message type or configuration key. The incremental store uses sequence-aware
JSON snapshots and JSONL journals; see [`reliability.md`](reliability.md) for
backup and rollback requirements.

## Startup memory

Recovery discovers snapshot and journal files, including sessions whose first
checkpoint has not been written. It scans snapshot `session_id`, `runs` and
sequence metadata, then applies newer journal run mutations. Event payloads
are streamed past without retaining their values; older snapshot field orders
remain readable. Snapshot sequence numbers prevent old journal records from
overriding the checkpoint.

Disk scanning does not add sessions to the store's resident session map.
Completed histories stay on disk until replay or a run update needs that
session. Replay and updates use the full snapshot-and-journal loader, preserving
events on the next save. An active session can therefore load its history once
execution resumes; this is not a bound on total runtime memory.

Sessions already cached in memory remain authoritative. A store with an empty
disk root still reports its in-memory active runs. Returned recovery records
copy the attachment slice and retain task IDs, parent IDs and iteration limits.

Hidden and temporary files are ignored. Unreadable metadata, inconsistent
session or sequence data, and middle-of-journal corruption skip the whole
session with a warning, rather than recovering stale snapshot state. A partial
or invalid final journal record is ignored as an uncommitted tail; the next
full load repairs it before a mutation. Startup scanning does not change files.

Metadata scanning is separate from full replay validation; a successful startup
scan does not prove the event history is intact. Hosts use `ReplayWithError` or
`ReplayForReconnectWithError` to surface damaged-history failures.

## Scope

No event history is deleted, no retention setting changes, and the reconnect
cursor behavior is preserved. This implementation has not been benchmarked for
RSS or startup duration, so no numerical memory reduction is claimed.
