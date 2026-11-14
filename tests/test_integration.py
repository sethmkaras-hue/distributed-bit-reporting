import asyncio
import time

import pytest
from conftest import eventually

from bitmesh.models import Command, Lifecycle


def cmd(cpu, node, kind, **arguments):
    return Command(
        target=node,
        boot_id=cpu.members[node]["boot_id"],
        kind=kind,
        operator="test-admin",
        arguments=arguments,
    )


async def test_live_threshold_archive_replay_and_dedup(cluster):
    sim, cpu = cluster
    target = "s01-n02"
    await sim.inject(target, "temperature", 92)
    await eventually(lambda: cpu.current[f"{target}/ps.temperature"]["value"] == 92)
    assert time.time() - cpu.current[f"{target}/ps.temperature"]["source_time"] < 1
    await eventually(lambda: any(e["category"] == "threshold" for e in cpu.archive.list("event", target)))
    events = cpu.archive.list("event", target)
    event = events[0]
    page = {"som_id": target, "boot_id": event["boot_id"], "events": [event]}
    before = len(cpu.archive.list("event", target))
    cpu.archive.ingest_events(page)
    cpu.archive.ingest_events(page)
    assert len(cpu.archive.list("event", target)) == before
    await sim.inject(target, "disconnect")
    a = sim.agents[target]
    a.event("test", "offline fault", "CRITICAL")
    await sim.inject(target, "reconnect")
    await eventually(
        lambda: any(e["description"] == "offline fault" for e in cpu.archive.list("event", target))
    )
    assert a.lifecycle == Lifecycle.QUARANTINED


async def test_failover_quarantine_nonpreemption_and_transfer(cluster):
    sim, cpu = cluster
    assert cpu.masters["S01"]["som_id"] == "s01-n01"
    initial_generation = sim.agents["s01-n01"].generation
    started = time.monotonic()
    await sim.inject("s01-n01", "master_failure")
    await eventually(lambda: cpu.masters.get("S01", {}).get("som_id") == "s01-n02", timeout=9)
    assert time.monotonic() - started < 10
    await sim.inject("s01-n01", "reconnect")
    await eventually(lambda: cpu.members["s01-n01"]["lifecycle"] == "QUARANTINED")
    await asyncio.sleep(0.5)
    assert cpu.masters["S01"]["som_id"] == "s01-n02"
    result = await cpu.dispatch(cmd(cpu, "s01-n01", "start"), "admin")
    assert result["state"] == "FAILED"
    result = await cpu.dispatch(cmd(cpu, "s01-n01", "approve"), "admin")
    assert result["state"] == "SUCCEEDED"
    await asyncio.sleep(0.5)
    assert cpu.masters["S01"]["som_id"] == "s01-n02"
    result = await cpu.dispatch(cmd(cpu, "s01-n02", "transfer", successor="s01-n01"), "admin")
    assert result["state"] == "UNKNOWN"
    await eventually(lambda: cpu.masters.get("S01", {}).get("som_id") == "s01-n01", timeout=9)
    assert sim.agents["s01-n01"].generation != initial_generation


async def test_commands_auth_idempotency_reset_and_pl_reconciliation(cluster):
    sim, cpu = cluster
    target = "s01-n03"
    c = cmd(cpu, target, "start")
    with pytest.raises(PermissionError):
        await cpu.dispatch(c, "viewer")
    result = await cpu.dispatch(c, "operator")
    assert result["state"] == "SUCCEEDED"
    assert await cpu.dispatch(c, "operator") == result
    c = cmd(cpu, target, "program_pl", artifact="sim-pl-2.0")
    result = await cpu.dispatch(c, "admin")
    assert result["state"] == "UNKNOWN"
    await eventually(
        lambda: any(x["id"] == c.id and x["state"] == "SUCCEEDED" for x in cpu.archive.list("command")),
        timeout=12,
    )
    assert cpu.archive.list("audit")
    c = cmd(cpu, target, "reset")
    before = sim.agents[target].boot_id
    assert (await cpu.dispatch(c, "admin"))["state"] == "UNKNOWN"
    await eventually(lambda: cpu.members[target]["boot_id"] != before)
    assert cpu.members[target]["lifecycle"] == "QUARANTINED"
    assert cpu.archive.list("gap", target)


async def test_split_brain_fails_closed_and_overflow_reported(cluster):
    sim, cpu = cluster
    # Fixed four-node voting set needs three; two surviving nodes cannot command.
    await sim.inject("s01-n03", "disconnect")
    await sim.inject("s01-n04", "disconnect")
    await asyncio.sleep(3.3)
    assert not any(a.authoritative for a in sim.agents.values())
    with pytest.raises(ValueError):
        await cpu.dispatch(cmd(cpu, "s01-n02", "start"), "admin")
    await sim.inject("s01-n03", "reconnect")
    await eventually(lambda: bool(cpu.masters), timeout=8)
    previous_sequence = sim.agents["s01-n02"].journal.sequence
    await sim.inject("s01-n02", "overflow", 100)
    assert sim.agents["s01-n02"].journal.sequence == previous_sequence + 100
    await eventually(lambda: bool(cpu.archive.list("gap", "s01-n02")))


async def test_cpu_reconnect_replays_retained_events(cluster):
    sim, cpu = cluster
    await cpu.close()
    sim.agents["s01-n02"].event("offline_cpu", "CPU was disconnected", "CRITICAL")
    await asyncio.sleep(0.3)
    assert sim.agents["s01-n01"].authoritative
    await cpu.start()
    await eventually(lambda: any(e["category"] == "offline_cpu" for e in cpu.archive.list("event")))


async def test_all_quarantined_bootstrap_approval(cluster):
    sim, cpu = cluster
    for agent in sim.agents.values():
        agent.lifecycle = Lifecycle.QUARANTINED
        agent.lease_until = 0
    await eventually(lambda: not cpu.masters)
    await eventually(lambda: all(m["lifecycle"] == "QUARANTINED" for m in cpu.members.values()))
    result = await cpu.dispatch(cmd(cpu, "s01-n01", "approve"), "admin")
    assert result["state"] == "SUCCEEDED"
    await eventually(lambda: cpu.masters.get("S01", {}).get("som_id") == "s01-n01")
    assert all(not a.eligible for n, a in sim.agents.items() if n != "s01-n01")


async def test_paged_snapshot_reconciliation(cluster):
    sim, cpu = cluster
    from bitmesh.agent.providers import default_parameters

    for agent in sim.agents.values():
        agent.parameters = default_parameters(200)
    await eventually(lambda: len(cpu.current) == 800, timeout=8)
    master = cpu.masters["S01"]["som_id"]
    node = cpu.by_id[master]
    first = await cpu.transport.call(node, "Snapshot", {"offset": 0}, timeout=1)
    assert len(first["measurements"]) == 512 and first["next_offset"] == 512
    second = await cpu.transport.call(node, "Snapshot", {"offset": 512}, timeout=1)
    assert len(second["measurements"]) == 288 and second["next_offset"] is None
    keys = {(m["som_id"], m["parameter"]["id"]) for m in first["measurements"] + second["measurements"]}
    assert len(keys) == 800


async def test_online_master_quarantine_does_not_block_successor(cluster):
    sim, cpu = cluster
    await sim.inject("s01-n01", "quarantine")
    await eventually(lambda: cpu.masters.get("S01", {}).get("som_id") == "s01-n02", timeout=9)
    assert sim.agents["s01-n01"].online and not sim.agents["s01-n01"].eligible


async def test_new_cpu_instance_detects_reboot_during_outage(cluster):
    from bitmesh.cpu.controller import Controller

    sim, cpu = cluster
    target = "s01-n02"
    old_boot = cpu.members[target]["boot_id"]
    await cpu.close()
    await sim.inject(target, "reboot")
    assert sim.agents[target].boot_id != old_boot
    replacement = Controller(sim.inventory, cpu.archive)
    await replacement.start()
    try:
        await eventually(
            lambda: any(
                g.get("boot_id") == sim.agents[target].boot_id for g in cpu.archive.list("gap", target)
            )
        )
        await eventually(lambda: replacement.members[target]["boot_id"] != old_boot)
        assert all(m["boot_id"] != old_boot for m in replacement.current.values() if m["som_id"] == target)
    finally:
        await replacement.close()
