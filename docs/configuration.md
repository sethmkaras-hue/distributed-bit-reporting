# Configuration reference and deployment

## Inventory

`config/simulation.yaml` is an editable, validated example. Nodes carry `id`, `subsystem`, numeric IPv4 physical `ip` and gRPC `endpoint`. Endpoints use loopback in simulation and real routable host:port pairs in production. Each subsystem has 4–16 consecutive physical IPs within one /24; IDs/endpoints/IPs are globally unique; SOM ID `cpu` is reserved for the CPU certificate identity. At most 25 subsystems and 400 nodes are accepted. Voting membership is fixed while running. Save changes to inventory and coordinate a complete subsystem restart; never independently shrink quorum during a partition.

`memory_mb`: 16–64 (default 16), allocated 50% event journal, 25% source cache/history, 12.5% master aggregate, remainder reserved for transport/control. For 500 parameters on 16-node masters use 64 MiB and validate actual object/RSS overhead on hardware. Sizes include serialized payload plus per-entry allowance, not whole Python process RSS. Queue counts and payload-size caps bound transport; this is not a proof of exact target RSS.

`lease_s`: 2–5 (default 3). Candidate authority expires 500 ms before voter grants; voters observe full lease blackout after process boot. `tick_s`: .05–.5 (default .2). `rpc_timeout_s` controls peer/read-only RPC deadlines from .1–.5 seconds (default .25). Do not reduce safety margin without target timing validation. Peer eligibility freshness is `max(2, 2 * peer_heartbeat_s + rpc_timeout_s)` seconds. `peer_heartbeat_s` is .2–2.0 (default 1.0): all members exchange full peer-health heartbeats at this interval. Peer loss is a connectivity observation; it does not let one returning minority peer quarantine a healthy majority. Current telemetry still forwards to the incumbent at `tick_s` between those health exchanges. Planned maintenance forces a full announcement immediately. Lease renewals are throttled independently of both telemetry and health heartbeats.

`parameters`: 1–500 parameter definitions. Omit for 12 mock definitions. Example policies in the YAML demonstrate PS/PL, resources and interface categories. Applying a policy through the GUI is fenced, validated and archived. It changes one targeted SOM; use the same category definitions for every desired SOM. Inventory changes, TLS and operator identity changes are deployment settings requiring restart, not live GUI writes.

## Environment variables

| Name | Purpose |
| --- | --- |
| BITMESH_DATABASE_URL | SQLAlchemy URL. Simulation defaults SQLite `.local/archive.db`; CPU defaults `postgresql+psycopg://bitmesh@127.0.0.1/bitmesh`. Credentials come from secure deployment settings. |
| BITMESH_TLS_CA | PEM dedicated service CA |
| BITMESH_TLS_CERT | PEM client/server certificate; SOM CN=stable ID, CPU CN=cpu; SAN matches endpoint |
| BITMESH_TLS_KEY | PEM private key path, permissions restricted by deployment |
| BITMESH_IDENTITIES | JSON token-to-identity map; each identity has `name` and `role` viewer/operator/admin. No default production token. |

Run hardware agents: `bitmesh agent --inventory /etc/bitmesh/inventory.yaml --som-id <stable-id>`. They start quarantined and report unsupported diagnostics until a real provider is integrated. Run CPU: `bitmesh cpu --inventory /etc/bitmesh/inventory.yaml`. Credentials/certificates must be configured first. Bind application HTTP to loopback behind HTTPS; do not expose plaintext remotely.

`deploy/compose.yaml` starts PostgreSQL for development with credentials supplied through secure environment settings. CPU services run on the host by default. `deploy/Caddyfile.example` demonstrates HTTPS termination and WebSocket forwarding; replace hostname and provision valid certificates. `deploy/bitmesh-agent.service` is an example systemd unit; install the package and configure identity/provider before enabling it. It does not provision boot images.

## Fault tolerance

CPU disconnect: collection and subsystem aggregation continue; bounded source RAM can overflow. CPU return: discovery, new snapshot, event replay and archive deduplication resume. CPU database unavailable: no event receipt is sent, journal remains retryable while RAM permits; measurement archival has a bounded queue and explicit drop metrics. Monitor `archive_error`, queue-drop counters and event gaps. CPU has no HA. Disk capacity and PostgreSQL backups are deployment responsibilities.

An all-quarantined startup supports a restricted direct CPU admin approval for one current boot. This is the only control exception to CPU-to-master routing and never authorizes normal processing without a quorum.
