# Implementation milestones

## 1 — Architecture and contracts

Established the monorepo, Pydantic BIT/event/command/inventory contracts, versioned protobuf/gRPC operations, fixed-membership lease safety design, initial Draw.io/PNG architecture and hardware assumptions before writing the services. Type, inventory, transition and voter unit tests validate the contract. JSON payloads in protobuf envelopes are a deliberate schema-evolution tradeoff, documented in interfaces.

## 2 — Distributed SOM agent

Implemented independent sampling, type checking, thresholds/clears, collection versus publication scheduling, deadbands, current cache/history, reserved bounded volatile journal, sequence gaps and archive receipts. Peer leases include reboot blackout, boot/generation fencing, non-preemptive renewal and closed behavior without majority. Test coverage includes overflow, quorum loss, returning quarantine, transfer and mTLS identity binding. Exact Python RSS on Kria remains a hardware gate; accounted buffers are not whole-process RAM.

## 3 — Central CPU

Implemented read-only candidate discovery with live grant verification, master-only live/replay sessions, paged snapshot reconciliation, persistent deduplicated event receipt, boot-gap reporting, measurement retention, command/approval/configuration/audit archives, authorization and REST/WebSocket. SQLite and a disposable PostgreSQL 17 instance were exercised. Hardware CPU clustering is intentionally absent. Archive schema migrations and large historical query pagination are deployment/future work.

## 4 — Simulation

Implemented configurable real-gRPC agents, mock BIT providers, fault/link/clock/overflow/reboot injections, PL communication outages and process-isolated subsystem workers. Injections alter source provider or link behavior, never CPU archive state. Tests cover CPU disconnect/replay and PS continuity during unavailable PL. Large-scale cold-start runs have exposed scheduling and traffic issues; every measured result is retained and separately labeled rather than assumed passing.

## 5 — Web console

Implemented React/TypeScript with MUI, responsive dark operations design, fleet cards, subsystem/SOM drilldown, parameter search/category filter, historical charts, fault severity filtering and acknowledgments/gaps, command confirmation/progress, policy editor, simulator controls and WebSocket reconnection. Frontend builds and live merge test pass. Inventory/endpoints/operator identities are configured in deployment settings rather than unsafely changing live voting membership.

## 6 — Reliability and integration

Implemented durable commit-before-receipt, idempotent event/command handling, independent replay/live paths, quarantine approval tied to current boot, direct CPU-only recovery approval for all-quarantined bootstrap, and post-reset/PL/update/transfer reconciliation with per-command completion receipts. Leadership tenures use new generations; CPU restart restores prior boot identities to detect offline reboot gaps. Deadline ambiguity stays UNKNOWN; it is never inferred successful from link loss. Automated tests include online-master quarantine, CPU reconnect, stale peer age and paged snapshots. See validation report for current measured performance gates and limitations.

## 7 — Documentation and validation

Added architecture/interfaces/configuration/operator/simulation/hardware guides, deployment examples, editable Draw.io plus rendered PNG, Mermaid state/sequence/deployment diagrams, frozen dependency locks, CI, browser smoke tooling and repeatable measured load tooling. Hardware PTP/Ethernet/programming/signed-artifact commissioning remains external. Final test counts and scale evidence are in validation.md and accompanying JSON, not inferred from this milestone log.
