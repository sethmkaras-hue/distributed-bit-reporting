import asyncio
import time

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event

from bitmesh.agent.providers import MockProvider
from bitmesh.agent.service import Agent
from bitmesh.cpu.api import create_app
from bitmesh.cpu.controller import Controller
from bitmesh.cpu.storage import Archive
from bitmesh.models import Event, Parameter
from bitmesh.simulation.runtime import make_inventory
from bitmesh.wire import pack, unpack


async def test_collection_publication_deadband_and_pl_sampling():
    inventory = make_inventory(subsystems=1)
    inventory.parameters = [
        Parameter(
            id="ps.temperature",
            name="Temperature",
            category="PS",
            data_type="float",
            collection_s=0.02,
            publication_s=5,
            deadband=10,
        ),
        Parameter(id="pl.temperature", name="PL", category="PL", data_type="float"),
    ]
    provider = MockProvider()
    provider.overrides["ps.temperature"] = 50.0
    agent = Agent(inventory.nodes[0], inventory, provider, bootstrap=True)
    await agent.sample_once()
    first = agent.cache.current["ps.temperature"][0].sequence
    agent.outbox.clear()
    await asyncio.sleep(0.03)
    await agent.sample_once()
    assert agent.cache.current["ps.temperature"][0].sequence > first
    assert "ps.temperature" not in agent.outbox  # collected without publication
    provider.overrides["ps.temperature"] = 80.0
    await asyncio.sleep(0.03)
    await agent.sample_once()
    assert agent.outbox["ps.temperature"]["value"] == 80
    agent.pl_unavailable = True
    agent.collected.clear()
    await agent.sample_once()
    assert agent.cache.current["pl.temperature"][0].validity == "UNAVAILABLE"
    assert agent.cache.current["ps.temperature"][0].validity == "VALID"
    await agent.transport.close()


def test_failed_commit_never_returns_archive_receipt(tmp_path):
    archive = Archive(f"sqlite:///{tmp_path}/archive.db")
    e = Event(som_id="a", boot_id="b", sequence=1, severity="CRITICAL", category="PS", description="fault")
    page = {"som_id": "a", "boot_id": "b", "events": [e.model_dump(mode="json")]}

    def reject_commit(conn):
        raise RuntimeError("disk failure")

    event.listen(archive.engine, "commit", reject_commit)
    with pytest.raises(RuntimeError, match="disk failure"):
        archive.ingest_events(page)
    event.remove(archive.engine, "commit", reject_commit)
    assert not archive.list("event")
    assert archive.ingest_events(page) == 1
    archive.close()


def test_wire_version_and_oversize():
    message = pack({"test": 1})
    assert unpack(message) == {"test": 1}
    message.schema_version = 2
    with pytest.raises(ValueError):
        unpack(message)
    with pytest.raises(ValueError):
        pack({"data": "x" * (4 * 1024 * 1024)})


def test_api_security_and_websocket_snapshot(tmp_path):
    inventory = make_inventory(subsystems=1)
    cpu = Controller(inventory, Archive(f"sqlite:///{tmp_path}/archive.db"))
    app = create_app(cpu, tokens={"test-token-not-for-deployment": {"name": "reader", "role": "viewer"}})
    with TestClient(app) as client:
        assert client.get("/api/state").status_code == 401
        headers = {"Authorization": "Bearer test-token-not-for-deployment"}
        assert client.get("/api/state", headers=headers).status_code == 200
        assert (
            client.post(
                "/api/commands", headers=headers, json={"target": "s01-n01", "boot_id": "x", "kind": "reset"}
            ).status_code
            == 403
        )
        assert (
            client.post(
                "/api/simulation/inject", headers=headers, json={"target": "s01-n01", "action": "disconnect"}
            ).status_code
            == 403
        )
        with client.websocket_connect("/api/live") as socket:
            socket.send_json({"token": "test-token-not-for-deployment"})
            assert socket.receive_json()["type"] == "state"


async def test_freshness_and_boot_scoped_approval(cluster):
    sim, cpu = cluster
    from bitmesh.models import Command

    old_boot = sim.agents["s01-n04"].boot_id
    await sim.inject("s01-n04", "disconnect")
    await asyncio.sleep(2.1)
    state = cpu.state()
    assert next(n for n in state["members"] if n["som_id"] == "s01-n04")["connection"] == "DISCONNECTED"
    assert (
        next(
            m
            for m in state["measurements"]
            if m["som_id"] == "s01-n04" and m["parameter"]["id"] == "ps.temperature"
        )["validity"]
        == "STALE"
    )
    c = Command(target="s01-n02", boot_id=old_boot, kind="approve", operator="admin")
    with pytest.raises(ValueError, match="boot identity"):
        await cpu.dispatch(c, "admin")
    c = Command(
        target="s01-n02",
        boot_id=cpu.members["s01-n02"]["boot_id"],
        kind="start",
        operator="admin",
        requested_at=time.time() - 100,
        timeout_s=1,
    )
    assert (await cpu.dispatch(c, "admin"))["state"] == "TIMED_OUT"


async def test_provider_type_fault_does_not_stop_other_collection():
    inventory = make_inventory(subsystems=1)
    inventory.parameters = inventory.parameters[:2]
    provider = MockProvider()
    provider.overrides["ps.temperature"] = "invalid-sensor-payload"
    agent = Agent(inventory.nodes[0], inventory, provider, bootstrap=True)
    await agent.sample_once()
    assert agent.cache.current["ps.temperature"][0].validity == "UNAVAILABLE"
    assert agent.cache.current["pl.temperature"][0].validity == "VALID"
    assert any(e.category == "provider" for e, _ in agent.journal.events)
    provider.overrides["ps.temperature"] = 49.0
    agent.collected.clear()
    await agent.sample_once()
    assert agent.cache.current["ps.temperature"][0].validity == "VALID"
    await agent.transport.close()


async def test_command_reconciliation_uses_monotonic_deadline(monkeypatch):
    from bitmesh.models import Command

    cpu = Controller(make_inventory(subsystems=1), None)
    command = Command(target="s01-n01", boot_id="old", kind="reset", operator="test", state="UNKNOWN")
    cpu.pending[command.id] = command.model_dump(mode="json")
    cpu.pending_deadlines[command.id] = time.monotonic() + 60
    monkeypatch.setattr(time, "time", lambda: command.requested_at + 10000)
    await cpu.reconcile()
    assert command.id in cpu.pending
    assert cpu.pending[command.id]["state"] == "UNKNOWN"
    await cpu.transport.close()


async def test_restart_requires_receipt_not_old_active_snapshot():
    from types import SimpleNamespace
    from bitmesh.models import Command

    written = []
    archive = SimpleNamespace(save=lambda *args: written.append(args))
    cpu = Controller(make_inventory(subsystems=1), archive)
    c = Command(target="s01-n01", boot_id="boot", kind="restart", operator="test", state="UNKNOWN")
    cpu.pending[c.id] = c.model_dump(mode="json")
    cpu.pending_deadlines[c.id] = time.monotonic() + 60
    cpu.members[c.target] = {"boot_id": "boot", "lifecycle": "ACTIVE", "connection": "CONNECTED"}
    cpu.received[c.target] = time.monotonic()
    await cpu.reconcile()
    assert c.id in cpu.pending and not written
    cpu.members[c.target]["operation_receipts"] = [{"id": c.id, "kind": "restart", "boot_id": "boot"}]
    await cpu.reconcile()
    assert c.id not in cpu.pending
    assert written[0][2]["state"] == "SUCCEEDED"
    await cpu.transport.close()
