"""Measured real-gRPC simulator load. Run from the repository root."""

import argparse
import asyncio
import json
import platform
import resource
import statistics
import time
from pathlib import Path

from bitmesh.cpu.controller import Controller
from bitmesh.cpu.storage import Archive
from bitmesh.simulation.runtime import ProcessSimulation, Simulation, make_inventory


async def main(args):
    inventory = make_inventory(args.subsystems, args.soms, args.parameters, args.port)
    inventory.memory_mb = 64
    inventory.tick_s = args.tick
    inventory.peer_heartbeat_s = args.peer_heartbeat
    inventory.lease_s = args.lease
    inventory.rpc_timeout_s = args.rpc_timeout
    if args.fast_ps_only:
        for parameter in inventory.parameters:
            if parameter.id != "ps.temperature":
                parameter.collection_s = 10
                parameter.publication_s = 10
    sim = ProcessSimulation(inventory) if args.isolated_subsystems else Simulation(inventory)
    directory = Path(".local")
    directory.mkdir(exist_ok=True)
    db = directory / f"benchmark-{time.time_ns()}.db"
    archive = Archive(f"sqlite:///{db}")
    cpu = Controller(inventory, archive)
    report = {
        "host": platform.platform(),
        "subsystems": args.subsystems,
        "soms": len(inventory.nodes),
        "parameters_per_som": args.parameters,
        "duration_requested_s": args.seconds,
        "transport": "real loopback gRPC; isolated subsystem processes"
        if args.isolated_subsystems
        else "real loopback gRPC; one Python asyncio process",
        "database": "SQLite WAL FULL",
        "buffer_budget_mb_per_som": 64,
        "heartbeat_and_forwarding_tick_s": args.tick,
        "full_peer_health_heartbeat_s": args.peer_heartbeat,
        "lease_s": args.lease,
        "peer_rpc_timeout_s": args.rpc_timeout,
        "started_at": time.time(),
        "sampling_profile": "PS temperature 200 ms; other categories 10 s"
        if args.fast_ps_only
        else "default",
    }
    started = time.monotonic()
    lag = []
    max_budget = 0
    try:
        await sim.start()
        await cpu.start()
        deadline = time.monotonic() + 45
        expected = len(inventory.nodes) * args.parameters
        while len(cpu.current) < expected and time.monotonic() < deadline:
            await asyncio.sleep(0.1)
        report["initialization_s"] = time.monotonic() - started
        report["initial_parameters_seen"] = len(cpu.current)
        report["expected_parameters"] = expected
        cpu.latencies.clear()
        cpu.fast_latencies = []
        until = time.monotonic() + args.seconds
        while time.monotonic() < until:
            tick = time.monotonic()
            await asyncio.sleep(0.1)
            lag.append(max(0, time.monotonic() - tick - 0.1))
            if not args.isolated_subsystems:
                max_budget = max(
                    max_budget,
                    max(a.cache.bytes + a.journal.bytes + a.aggregate_bytes for a in sim.agents.values()),
                )
        stats = (
            await sim.stats()
            if args.isolated_subsystems
            else {
                n: {
                    "buffer_bytes": a.cache.bytes + a.journal.bytes + a.aggregate_bytes,
                    "queue_drops": a.queue_drops,
                    "lifecycle": a.lifecycle,
                    "journal_pending": len(a.journal.events),
                    "event_sequence": a.journal.sequence,
                    "failures": [
                        str(t.exception())
                        for t in a.tasks
                        if t.done() and not t.cancelled() and t.exception()
                    ],
                }
                for n, a in sim.agents.items()
            }
        )
        max_budget = max(max_budget, max(x["buffer_bytes"] for x in stats.values()))
        latencies = sorted(cpu.fast_latencies)
        fast_values = [m for m in cpu.current.values() if m["parameter"]["collection_s"] <= 0.2]
        fresh_fast_sources = {m["som_id"] for m in fast_values if time.time() - m["source_time"] < 1}
        archived_events = await asyncio.to_thread(archive.list, "event", limit=100000)
        event_keys = {(e["som_id"], e["boot_id"], e["sequence"]) for e in archived_events}
        report.update(
            fresh_fast_som_count=len(fresh_fast_sources),
            quarantined_som_count=sum(x["lifecycle"] == "QUARANTINED" for x in stats.values()),
            archived_events=len(archived_events),
            duplicate_archived_events=len(archived_events) - len(event_keys),
            pending_source_events=sum(x["journal_pending"] for x in stats.values()),
            measured_source_to_cpu_samples=len(latencies),
            source_to_cpu_p50_ms=statistics.median(latencies) * 1000 if latencies else None,
            source_to_cpu_p95_ms=latencies[int(len(latencies) * 0.95)] * 1000 if latencies else None,
            event_loop_lag_p95_ms=sorted(lag)[int(len(lag) * 0.95)] * 1000 if lag else None,
            archive_queue_drops=cpu.archive_drops,
            source_queue_drops=sum(x["queue_drops"] for x in stats.values()),
            max_accounted_som_buffer_bytes=max_budget,
            process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            connected_masters=len(cpu.masters),
            task_failures=[failure for x in stats.values() for failure in x["failures"]],
        )
        victim = cpu.masters.get("S01", {}).get("som_id")
        if victim:
            failed_at = time.monotonic()
            await sim.inject(victim, "master_failure")
            while time.monotonic() - failed_at < 12:
                new = cpu.masters.get("S01", {}).get("som_id")
                if new and new != victim:
                    report["master_recovery_s"] = time.monotonic() - failed_at
                    break
                await asyncio.sleep(0.05)
            else:
                report["master_recovery_s"] = None
        report["elapsed_s"] = time.monotonic() - started
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        report["elapsed_s"] = time.monotonic() - started
    finally:
        await cpu.close()
        await sim.close()
        archive.close()
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))
    passed = (
        not report.get("error")
        and report.get("initial_parameters_seen") is not None
        and report.get("initial_parameters_seen") == report.get("expected_parameters")
        and report.get("connected_masters") == args.subsystems
        and report.get("fresh_fast_som_count") == len(inventory.nodes)
        and report.get("quarantined_som_count") == 0
        and report.get("duplicate_archived_events") == 0
        and report.get("max_accounted_som_buffer_bytes", 10**10) <= inventory.memory_mb * 1024 * 1024
        and report.get("source_to_cpu_p95_ms") is not None
        and report["source_to_cpu_p95_ms"] < 1000
        and (report.get("master_recovery_s") or 99999) < 10
        and not report.get("task_failures")
    )
    raise SystemExit(0 if passed else 1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--isolated-subsystems", action="store_true")
    p.add_argument("--fast-ps-only", action="store_true")
    p.add_argument("--subsystems", type=int, default=3)
    p.add_argument("--soms", type=int, default=4)
    p.add_argument("--parameters", type=int, default=100)
    p.add_argument("--lease", type=float, default=3.0)
    p.add_argument("--rpc-timeout", type=float, default=0.25)
    p.add_argument("--peer-heartbeat", type=float, default=1.0)
    p.add_argument("--tick", type=float, default=0.2)
    p.add_argument("--seconds", type=float, default=10)
    p.add_argument("--port", type=int, default=52000)
    p.add_argument("--output", default="docs/performance-local.json")
    asyncio.run(main(p.parse_args()))
