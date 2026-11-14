import pytest
from pydantic import ValidationError

from bitmesh.agent.election import Voter
from bitmesh.agent.journal import Cache, Journal
from bitmesh.agent.providers import default_parameters
from bitmesh.models import Lifecycle, Measurement, transition
from bitmesh.simulation.runtime import make_inventory


def test_types_and_inventory():
    p = default_parameters()[0]
    with pytest.raises(ValidationError):
        Measurement(som_id="a", boot_id="b", parameter=p, value=True)
    with pytest.raises(ValidationError):
        make_inventory(soms=3)
    inventory = make_inventory(subsystems=1).model_dump(mode="json")
    inventory["nodes"][0]["id"] = "cpu"
    from bitmesh.models import Inventory

    with pytest.raises(ValidationError, match="reserved"):
        Inventory.model_validate(inventory)
    with pytest.raises(ValueError):
        transition(Lifecycle.QUARANTINED, Lifecycle.READY)
    assert transition(Lifecycle.QUARANTINED, Lifecycle.READY, approved=True) == Lifecycle.READY


def test_volatile_overflow_and_ack():
    j = Journal(1600)
    for _ in range(20):
        j.append(som_id="a", boot_id="b", severity="CRITICAL", category="test", description="fault")
    assert j.bytes <= j.budget
    assert j.overwritten > 0 and j.replay()["gaps"][0][0] == 1
    assert j.replay()["events"][-1]["sequence"] == 20
    j.ack(20)
    assert j.bytes == 0
    with pytest.raises(ValueError):
        j.ack(21)


def test_cache_bounded():
    c = Cache(1800)
    for p in default_parameters():
        m = Measurement(
            som_id="a",
            boot_id="b",
            parameter=p,
            value=1 if p.data_type in {"float", "integer"} else True if p.data_type == "boolean" else "v",
        )
        c.put(m)
    assert c.bytes <= c.budget and c.dropped > 0


def test_voter_blackout_nonoverlap_and_boot_fencing():
    clock = [0.0]
    v = Voter(3, now=lambda: clock[0])
    assert not v.vote("a", "boot1", "gen1", "a")["granted"]
    clock[0] = 3.1
    assert v.vote("b", "boot2", "gen2", "b")["granted"]
    assert not v.vote("a", "boot1", "gen1", "a")["granted"]
    assert not v.verify("b", "wrong", "gen2")
    assert v.vote("b", "boot2", "gen2", "a")["granted"]  # nonpreemptive incumbent
    clock[0] = 6.2
    assert v.vote("a", "boot1", "gen3", "a")["granted"]
    assert not v.verify("b", "boot2", "gen2")


async def test_near_expiry_grants_cannot_authorize_new_operations():
    import time
    from unittest.mock import AsyncMock
    from bitmesh.agent.service import Agent
    from bitmesh.agent.providers import MockProvider
    from bitmesh.cpu.controller import Controller

    inventory = make_inventory(subsystems=1)
    a = Agent(inventory.nodes[0], inventory, MockProvider(), bootstrap=True)
    authority = {
        "candidate": a.node.id,
        "boot_id": a.boot_id,
        "generation": a.generation,
        "grantors": [n.id for n in inventory.nodes],
    }
    a.voter.blackout_until = time.monotonic() - 1
    a.voter.vote(a.node.id, a.boot_id, a.generation, a.node.id)
    a.transport.call = AsyncMock(return_value={"valid": True, "valid_for_s": 0.1})
    cpu = Controller(inventory, None)
    cpu.transport.call = AsyncMock(return_value={"valid": True, "valid_for_s": 0.1})
    assert not await a.verify(authority)
    assert not await cpu.authority_valid("S01", authority)
    a.transport.call.return_value = {"valid": True, "valid_for_s": 3}
    cpu.transport.call.return_value = {"valid": True, "valid_for_s": 3}
    assert await a.verify(authority)
    assert await cpu.authority_valid("S01", authority)
    await a.transport.close()
    await cpu.transport.close()
