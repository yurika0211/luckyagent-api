# WebSocket run recovery

`websocket.RunStore.Restoreable` returns queued and running runs for the host to
resume. The host still owns execution and startup. This change adds no route,
message type, configuration key or persisted JSON field.

## Startup memory

Recovery scans the `session_id` and `runs` fields using a JSON decoder. The
current writer places those fields before `events`, so the scanner returns
before decoding event history. Older field orders remain readable: unknown
fields, including events, are streamed past without retaining their values.

Disk scanning does not add sessions to the store's resident session map.
Completed histories stay on disk until replay or a run update needs that
session. Replay and updates use the existing full-session loader, preserving
events on the next save. An active session can therefore load its history once
execution resumes; this is not a bound on total runtime memory.

Sessions already cached in memory remain authoritative. A store with an empty
disk root still reports its in-memory active runs. Returned recovery records
copy the attachment slice and retain task IDs, parent IDs and iteration limits.

Hidden and temporary JSON files are ignored. Unreadable or invalid recovery
metadata is skipped. The fast path intentionally does not validate the event
tail; replay uses the existing full JSON loader.

## Scope

No event history is deleted, no retention setting changes, and the reconnect
cursor behavior is preserved. This implementation has not been benchmarked for
RSS or startup duration, so no numerical memory reduction is claimed.
