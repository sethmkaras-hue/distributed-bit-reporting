# BITMesh

Distributed Built-In Test telemetry, volatile SOM event journals, fenced subsystem leadership and a central operations console for AMD Kria integration.

**Hardware-independent development implementation.** The simulator runs real gRPC agents, CPU archival and command reconciliation. Hardware adapters intentionally reject unsupported operations. See [hardware commissioning gates](docs/hardware-integration.md) and [validation results](docs/validation.md) before production use.

## Local development

Python 3.12+, Node 22+ and `uv` are required. Work in the existing checkout; an isolated task does not need another Git worktree.

```bash
export UV_CACHE_DIR="$PWD/.local/uv-cache"
uv sync --frozen --extra dev
npm --prefix web ci --cache "$PWD/.local/npm-cache"
npm --prefix web run build
uv run --frozen bitmesh simulate --subsystems 3 --soms 4 --parameters 100
```

The CPU serves the built console and API on loopback port 8000. The default simulation has three subsystems with four SOMs each; use `--soms 16 --subsystems 25 --isolated-subsystems` for separate subsystem worker processes. No credentials are required in loopback simulation. Stop with Ctrl-C. Data persists in `.local/archive.db`; simulated SOM RAM is destroyed when the process exits. Do not expose the unauthenticated simulator remotely.

For frontend development, keep the simulator running and use `npm --prefix web run dev`. Vite proxies API and WebSocket traffic to the CPU. Use `--grpc-port` to run multiple independent simulations on nonoverlapping ports.

```bash
uv run --frozen pytest -q
npm --prefix web test
npm --prefix web run build
uv run --frozen ruff check bitmesh tests tools --exclude 'mesh_pb2*.py'
uv run --frozen python tools/benchmark.py --subsystems 3 --soms 4 --parameters 100 --seconds 10
# Resource-intensive, run separately from tests and browsers:
uv run --frozen python tools/benchmark.py --isolated-subsystems --subsystems 25 --soms 16 --parameters 100 --seconds 30 --output docs/performance-400.json
```

The optional browser smoke script requires Python Playwright and a Chromium executable (`/usr/bin/chromium` in the development cloud machine). Start the simulator first, then `python tools/browser_smoke.py`. It checks real browser fault navigation and reconnect, and saves screenshots. It is separate from the Python unit/integration suite.

## Repository

| Area | Purpose |
| --- | --- |
| `bitmesh/agent` | Identical SOM service, providers, sampling, lease fencing, bounded RAM |
| `bitmesh/cpu` | Master discovery, SQL archive, REST/WebSocket, authorization and commands |
| `bitmesh/simulation` | Shared-interface agents and optional subsystem worker processes |
| `protocol` | Versioned gRPC contracts; generated bindings are checked in |
| `web` | React/TypeScript, MUI and Recharts console |
| `config` | Validated example inventories |
| `deploy` | PostgreSQL, HTTPS proxy and systemd examples |
| `tests`, `tools` | Acceptance checks and measured load/browser tools |
| `docs` | Architecture, editable diagram, guides and evidence |

## Documentation

- [Architecture, safety and assumptions](docs/architecture.md)
- [Interfaces and schema](docs/interfaces.md)
- [State, replay, command, PL and network diagrams](docs/diagrams/flows.md)
- [Editable Draw.io architecture](docs/diagrams/architecture.drawio) · [PNG](docs/diagrams/architecture.png)
- [Configuration and deployment](docs/configuration.md)
- [Operator guide](docs/operator-guide.md)
- [Simulation guide](docs/simulation.md)
- [Hardware integration](docs/hardware-integration.md)
- [Test, performance and milestone report](docs/validation.md)

This repository does not implement boot image provisioning, CPU redundancy or firmware rollback. There is no guaranteed recovery of unarchived events lost at reboot or journal overflow. No physical timing or hardware performance claims are made.
