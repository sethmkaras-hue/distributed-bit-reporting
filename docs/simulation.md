# Simulation guide

`bitmesh simulate` creates independent Agent service instances with loopback gRPC endpoints, MockProvider objects and separate volatile stores. The CPU uses exactly the same Identity, grant-challenge, Live, Events, Snapshot, Command and archive paths as hardware mode. It never copies simulated provider state directly into CPU current state or SQL. Optional `--isolated-subsystems` spawns one process per subsystem. Agents remain BOOTING during worker construction, then an explicit simulator-only activation barrier starts eligibility; it never approves returning quarantined nodes. fault injection IPC changes source provider/link state only.

Default topology is 3 × 4 SOMs, each with 12 parameters. Set `--parameters 100` or `500` for load, `--subsystems 25 --soms 16` for max inventory, and use isolated subsystem workers at scale. No hardware or external services are needed for the SQLite workflow. Separate simulation processes do not survive application shutdown; CPU archive does.

Use GUI controls or authenticated `POST /api/simulation/inject` with `target`, `action`, `value`. Available actions:

| Action | Value / effect |
| --- | --- |
| temperature | numeric PS temperature; thresholds create real journal events |
| cpu, memory | utilization numeric override |
| interface | integer interface error counter |
| disconnect, master_failure | link off and source FAILED; PS sampling continues |
| reconnect | link restored; source QUARANTINED |
| quarantine | online source loses processing/master eligibility; successor election proceeds |
| reboot | new boot ID, source journal/cache loss, voting blackout and quarantine |
| overflow | 1–10000 fault events in a deliberately reduced 4 KiB source journal |
| ptp | SYNCHRONIZED, DEGRADED or UNSYNCHRONIZED quality |

Normal start/stop/restart/reset/update/PL/approval/transfer actions use `/api/commands`, not special simulation shortcuts. Mock artifact operations require a `sim-*` identity. PL outage marks PL telemetry unavailable and disconnects Ethernet for one second while independent PS sampling runs. Reset causes a new boot and unarchived history loss. Mock clock quality is metadata, not simulated kernel PHC synchronization.

Run tests and load measurements separately to avoid host saturation contaminating deadline-sensitive results. `tools/benchmark.py` reports full inventory coverage, actual samples, loop lag, CPU latency, queue drops, accounted buffers and failover time. A nonzero exit means its gates did not pass; inspect saved JSON. Tests use real TCP gRPC sockets. Browser smoke requires a running built simulator and Playwright; it exercises UI navigation, injected faults, reconnect and mobile layout. It does not claim hardware PTP accuracy or full 400-SOM browser throughput.
