# Implemented state and sequence diagrams

## Master failover
```mermaid
stateDiagram-v2
    [*] --> StartupBlackout
    StartupBlackout --> Follower: full lease duration elapsed
    Follower --> Candidate: lowest eligible reachable numeric IP
    Candidate --> Master: fixed-inventory majority grants lease
    Candidate --> Follower: no majority
    Master --> Master: renew same boot and generation
    Master --> Follower: lease expires or transfer stops renewals
    Follower --> Quarantined: local total-isolation or explicit recovery condition
    Quarantined --> Follower: operator approves current boot
```

## SOM operational lifecycle
Connection CONNECTED/DISCONNECTED and role MASTER/FOLLOWER are orthogonal to this lifecycle.
```mermaid
stateDiagram-v2
    [*] --> BOOTING
    BOOTING --> QUARANTINED: production startup
    BOOTING --> READY: explicit simulator bootstrap
    QUARANTINED --> READY: current boot approved
    READY --> ACTIVE: start
    ACTIVE --> READY: stop
    READY --> MAINTENANCE: planned operation
    ACTIVE --> MAINTENANCE: planned operation
    MAINTENANCE --> READY: verified simulated PL/software operation
    MAINTENANCE --> QUARANTINED: reboot
    READY --> FAILED: injected failure
    ACTIVE --> FAILED: injected failure
    FAILED --> QUARANTINED: return
```

## Archive and replay
```mermaid
sequenceDiagram
    participant S as Source SOM journal
    participant M as Master
    participant C as CPU
    participant D as Archive database
    C->>M: Events stream (separate from Live)
    loop bounded pages of 128
        M->>S: Replay
        S-->>M: boot / sequences / retained events / gaps
        M-->>C: event page
        C->>D: INSERT ON CONFLICT DO NOTHING + gap records
        D-->>C: COMMIT succeeded
        C->>M: Acknowledge source boot and sequence
        M->>S: relay Acknowledge
        S->>S: release acknowledged journal entries
    end
    Note over S,D: disconnect retries retained events; reboot destroys unarchived RAM
```

## Command processing
```mermaid
sequenceDiagram
    actor O as Operator
    participant G as GUI
    participant C as CPU
    participant V as Fixed voting peers
    participant M as Master
    participant S as Target SOM
    O->>G: request and confirm disruptive operation
    G->>C: authenticated request / command UUID / boot ID
    C->>C: authorize and validate
    C->>V: challenge current master grants
    V-->>C: quorum confirms live generation
    C->>C: persist accepted request and audit
    C->>M: Command + authority
    M->>V: validate live grants
    M->>S: route command
    S->>V: independently validate grants
    S->>S: validate boot, quarantine and deadline; execute adapter
    S-->>C: result via master (UNKNOWN for reset/PL/update/transfer)
    C->>C: reconcile later snapshot; persist outcome
    C-->>G: WebSocket update
```

## PL programming
```mermaid
sequenceDiagram
    participant C as CPU
    participant M as Master
    participant S as Target SOM
    participant P as PS collector
    C->>M: Program PL (non-master target)
    M->>S: fenced command with artifact identity
    S->>S: mock provider validates sim-* artifact
    S-->>C: UNKNOWN via master
    S->>M: announce MAINTENANCE
    S->>S: Ethernet unavailable, PL values unavailable
    P->>P: continue PS BIT and RAM journal
    S->>S: restore link and READY
    C->>M: refresh snapshot and replay events
    M-->>C: post-operation PL identity and matching completion receipt
    C->>C: compare expected artifact, boot and operation receipt
    C->>C: mark SUCCEEDED only after reconciliation
```

## Network and deployment
```mermaid
flowchart TB
    Browser[Operator browser] -->|HTTPS REST and WebSocket| Proxy[TLS reverse proxy]
    Proxy -->|loopback HTTP| CPU[CPU service]
    CPU --> PG[(PostgreSQL)]
    CPU -->|CPU initiated mTLS gRPC| Master[Current subsystem master]
    CPU -.->|read only identity discovery and grant challenges| Peers[Inventory candidate addresses]
    Master <-->|mTLS peer RPC| Peers
    Peers --> RAM[Per-SOM volatile caches and journals]
    Peers --> Provider[Hardware provider boundary]
    Provider --> PS[PS independent sampling]
    Provider --> PL[PL Ethernet and telemetry]
```
