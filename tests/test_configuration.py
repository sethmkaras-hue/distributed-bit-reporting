import asyncio

from bitmesh.models import Command
from conftest import eventually


async def test_policy_validation_is_atomic_and_archived(cluster):
    sim, cpu = cluster
    target = "s01-n02"
    parameters = [p.model_dump(mode="json") for p in sim.agents[target].parameters]
    original = len(parameters)
    c = Command(
        target=target,
        boot_id=cpu.members[target]["boot_id"],
        kind="configure",
        operator="admin",
        arguments={"parameters": []},
    )
    assert (await cpu.dispatch(c, "admin"))["state"] == "FAILED"
    assert len(sim.agents[target].parameters) == original
    for p in parameters:
        if p["category"] == "Resources":
            p["collection_s"] = 0.4
            p["publication_s"] = 0.8
            p["retention_s"] = 60
    c = Command(
        target=target,
        boot_id=cpu.members[target]["boot_id"],
        kind="configure",
        operator="admin",
        arguments={"parameters": parameters},
    )
    assert (await cpu.dispatch(c, "admin"))["state"] == "SUCCEEDED"
    assert next(p for p in sim.agents[target].parameters if p.category == "Resources").collection_s == 0.4
    assert cpu.archive.list("configuration", target)
    await eventually(lambda: cpu.current[f"{target}/cpu.utilization"]["parameter"]["publication_s"] == 0.8)
    await asyncio.sleep(0.1)
