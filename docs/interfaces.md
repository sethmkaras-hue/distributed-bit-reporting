# Interfaces and schemas

The authoritative runtime schemas are in `bitmesh/models.py`. Wire version 1 uses `protocol/mesh.proto` Envelope messages containing schema-validated JSON. Numbers must be finite; values are checked against their declared boolean/integer/float/string/enum/bitfield type. IDs are stable and independent of IP address. Descriptive metadata is sent with measurements for self-describing snapshots. Production deployments should benchmark bandwidth before introducing a negotiated metadata dictionary.

## Agent gRPC

| Operation | Caller and semantics |
| --- | --- |
| Peer | Inventory peers: heartbeats, fixed-member votes, live grant challenges and transfer announcements. CPU may challenge grants read-only. |
| Identity | CPU discovery: SOM, subsystem, boot, lifecycle, role and expiring authority. An assertion alone is insufficient; CPU challenges the configured voters. |
| Snapshot | CPU to authoritative master; membership plus up to 512 measurements/page (`offset`, `next_offset`). |
| Live | CPU-initiated current telemetry stream, max 2 clients, bounded 512-entry queues, batches <=128, drop oldest with counter; reconcile via Snapshot. |
| Events | Independent CPU-initiated critical/history stream, relay pages <=128 per source. Unacked pages may repeat. |
| Replay | Master to source: retained journal page, boot ID, sequence gaps and loss accounting. |
| Acknowledge | CPU after durable commit, relayed by master to source; source checks matching boot ID and sequence range. This is archive receipt, not operator fault acknowledgment. |
| Command | CPU to verified master to target. Normal command preflight refreshes identity against that master only; peers are challenged directly during read-only discovery, while master and target independently challenge grants for command fencing. Both master and target verify live quorum authority; target checks boot, lifecycle, deadline and cached command ID. Live grant challenges include remaining validity and a conservative RTT/execution margin, and are repeated after acquiring the target lock. |
| Configure | Same fenced command validation as Command; no separate unauthenticated write path. |

Maximum envelope payload is 4 MiB. Insecure transport is loopback-only. Production enables mTLS and checks certificate common names against the invoking stable identity; dedicated CA trust is required. Lease safety assumes trusted non-Byzantine participants. Normal telemetry connections terminate at masters; discovery/grant challenges are read-only exceptions. Peer loss is detected using monotonic observation age. Telemetry forwards to the master at the inventory tick; independent one-second full peer-health heartbeats maintain candidate visibility. Live and replay streams cannot form an unbounded queue.

## BIT

Parameter: `id`, `name`, `description`, `category`, `data_type`, `unit`, `collection_s`, `publication_s`, `retention_s`, `deadband`, `warning`, `critical`. Policies may differ by category and by SOM. Collection uses monotonic intervals. Publication is periodic or change/deadband/threshold driven, with periodic full reconciliation. GUI batches deltas at 100 ms. Changing policy resets local measurement cache; policies are CPU-archived but volatile on the SOM and must be re-applied after boot.

Measurement: stable `som_id`, `boot_id`, definition, typed `value`, `sequence`, source/master/CPU wall times, validity VALID/UNAVAILABLE/STALE, severity INFO/WARNING/CRITICAL and PTP quality. GUI records source-to-render-processing latency for fast synchronized batches. Cross-machine latency measurements require synchronized clocks. Monotonic observation age governs CPU freshness. Numeric thresholds are upper-bound thresholds; boolean/enumeration custom fault rules require a provider extension.

## Events

Key: `(som_id, boot_id, sequence)`. Fields: source wall time, severity, category, description, parameter ID, state ACTIVE/CLEARED/NOTICE, PTP state/offset/uncertainty/grandmaster. Threshold clear generates a separate CLEARED event. Source journal is bounded and volatile. Acknowledged entries may be freed; overwrites of unarchived entries produce contiguous missing ranges, cumulative loss count and bounded range metadata with explicit range-metadata loss accounting. CPU stores events and gaps in one commit before returning receipt. Duplicate inserts are idempotent. Operator acknowledgments are independent audited records.

## Commands

Request: stable UUID, target SOM, current boot, type, authenticated operator, request wall time, deadline and validated arguments. CPU authorization supplies identity; the browser cannot override it. State is ACCEPTED then returned terminal state or UNKNOWN while reconciling. Reset, PL, software update and transfer return UNKNOWN initially. PL/software completion requires both expected artifact metadata and a matching post-operation receipt for the same command and boot; an old READY snapshot cannot establish completion. A dropped connection proves no outcome. CPU restarts recover pending commands and prior SOM boot identities from persistent rows. Reconciliation deadlines use monotonic time; UTC age is converted once at ingress/restart. Reusing a command ID with a different payload is rejected; CPU looks up all archived IDs, not just the recent display window. Source command result cache is bounded at 256 entries; do not use it as a durable exactly-once guarantee. No automatic replay of uncertain disruptive commands occurs.

## Browser REST and WebSocket

`GET /api/state`, `GET /api/archive/{kind}?som_id=...&limit=...`, `POST /api/commands`, `POST /api/events/{som}/{boot}/{sequence}/ack`, simulator-only `POST /api/simulation/inject`. Swagger describes request schemas at `/docs`.

`/api/live`: first WebSocket frame `{ "token": "..." }`; no bearer tokens in query URLs. Initial state, telemetry deltas, membership refresh, event/command notices and heartbeat frames follow. Queue bound is 16 messages/client; slow clients drop old updates and get reconciliation. The browser reconnects automatically, refreshes state and batches renders. Archive queries are capped at 2000 rows; older records remain in the DB. Paginated historical browsing beyond this recent window is a future enhancement.

SQLAlchemy uses SQLite or PostgreSQL. Event, gap, measurement, inventory, command, configuration, approval, connectivity, leadership, acknowledgment and audit records share a typed-kind table with unique kind/key constraints and time/source indexes. Production migrations are required before altering this initial schema. Measurement retention expires by per-parameter category policy; fault/audit retention is intentionally archival and needs deployment disk monitoring.

Recovery exception: when no verified master exists, CPU admin approval may be sent directly to the discovered current-boot target using the CPU certificate. Only the `approve` command bypasses leadership verification, and only for CPU identity and self target. It changes quarantine eligibility, not processing state. All operational commands still require majority fencing. This is necessary for an all-quarantined subsystem to bootstrap without assuming persistent SOM approval.

Transport channels retain one generated AgentStub each and bound message sizes. Quarantine is based on explicit failed/reboot lifecycle or local return from total peer isolation; a remote peer report is not authority to quarantine a healthy node. Reintegration refreshes peer contact before enabling READY.
