# Validation and acceptance status

This is an executable development implementation, **not a completed production acceptance**. Full-scale 400-SOM operation has not passed. No physical Kria, PTP or PL Ethernet accuracy claim is made.

## Automated checks

The latest full Python run executed 24 tests with 0 failures and 0 errors; see `test-results.xml`. Coverage includes typed values/inventory, lifecycle approval, reboot voting blackout, exclusive leases and expiry margins, generation changes on leadership reacquisition, failover/non-preemption/transfer, split-brain command rejection, sampling/publication/deadbands, provider failures, PL/PS independence, bounded journals/caches, overflow sequence continuity and gaps, durable commit-before-receipt, replay/deduplication, command authorization/idempotency/receipts, monotonic deadlines, TLS identity checks, policy validation, paged snapshots and CPU replacement detecting a SOM reboot during its outage. Earlier stress and concurrent-service runs failed; the final suite runs in isolation. Deadline-sensitive liveness checks must not be run alongside the scale benchmark.

Python lint passes. The TypeScript/Vite production build and one frontend reconciliation test pass. Vite reports a bundle-size advisory; code splitting is a future optimization. FastAPI TestClient emits an upstream httpx deprecation warning; the tests executed successfully.

PostgreSQL 17 validation passed durable receipts, deduplication, reopening committed events, maximum-length measurement keys and persistent last-boot lookup. See `postgres-validation.json`. The disposable container was removed and no generated credentials were retained. The readiness probe uses TCP so PostgreSQL's temporary bootstrap socket cannot be mistaken for the final server.

## Measured smaller load

`performance-16.json`: 16 SOMs, 100 parameters each, 1600 of 1600 values observed. The 5-second steady measurement observed 16 SOMs with fresh fast-category data, 214 ms source-to-CPU p95, 2.85 s master recovery, 0 archive queue drops and 0 source queue drops. Maximum accounted cache/journal/aggregation bytes were 2,033,442. All 33 archived events were unique; pending source events were 0. This short local run is not a sustained-load or target-hardware certification. Parent RSS excludes worker processes; byte-accounted buffers are not a proof of total Python/C-core RSS.

## Full-scale gate — failed

The machine has a four-core cgroup quota (`cpu.max = 400000 100000`). `performance-400.json` records the latest 25-subsystem / 400-SOM / 100-parameter real-gRPC attempt. It used PS collection/publication at 200 ms, other categories at 10 seconds, forwarding at 500 ms, peer health at 2 s, leases at 5 s and peer RPC deadlines at 500 ms. The 1 s visibility and 10 s failover acceptance thresholds were retained.

Observed coverage was 13,108 of 40,000; fresh fast-data SOM count was 0, verified master sessions were 3, and 309 SOMs entered quarantine. Fast-category CPU p95 was 6092.494964599609 ms and CPU event-loop p95 lag was 1022.7279309987351 ms. The run failed. Retained `performance-400-*-failed.json` reports show earlier startup, scheduling and storage failures; SQLite write contention also occurred at this scale. These are diagnosis evidence, not passes.

The implementation now caches generated RPC wrappers per channel, separates full peer health from fast telemetry forwarding, keeps CPU DB work off the event loop, batches ordinary historical writes, uses a simulator boot barrier and avoids single-peer quarantine authority. These corrections passed focused/integration tests but **do not establish 400-SOM readiness**. Further profiling, resource provisioning and a representative PostgreSQL load run are required; more CPU alone is not proven sufficient. No 400-SOM GUI latency, 500-parameter maximum-load or long-duration performance claim is made.

## Browser evidence

`browser/report.json` and screenshots record an actual Chromium run against three subsystems and twelve agents with 100 parameters per SOM. Injected PS temperature reached the DOM in 845 ms. There were 0 page errors, 2 WebSocket connections demonstrating reconnect, a working disruptive-command confirmation dialog, source-to-fault navigation and no horizontal document overflow at 390 px. This measures one appropriately configured fast category on the small system, not every category or maximum scale.

## Remaining work and limits

- Pass full-scale and sustained tests, including database pressure, replay alongside live traffic, partitions, disk-full recovery and maximum-scale GUI rendering.
- Integrate actual Kria sensors, process supervision/stop-on-quarantine, reset, signed software/PL artifacts and post-operation verification. Hardware mode currently rejects unsupported controls.
- Validate PTP within 100 microseconds and PS continuity/PL Ethernet recovery on the final implementation. No boot-image provisioning, CPU HA or rollback is implemented.
- Confirm exact target RAM/RSS and transport allocation; serialized buffer accounting does not certify the complete 16–64 MiB target.
- Bulk subsystem command orchestration is not implemented. Inventory/network/operator identity edits require deployment configuration and coordinated restart; the GUI edits one SOM's monitoring policy at a time.
- Historical queries expose a recent bounded window (up to 2,000 rows); large-history pagination, archival schema migrations and richer fault-episode aggregation remain. Current BIT violations and historical RAISED/CLEARED events are presented separately.

The code, simulator, archive, dashboard, editable diagrams, deployment examples and tests are available for review and further development. Publication status is separate from validation; publication does not imply production acceptance.
