import asyncio
import socket
import time

import pytest_asyncio

from bitmesh.cpu.controller import Controller
from bitmesh.cpu.storage import Archive
from bitmesh.simulation.runtime import Simulation, make_inventory


def free_port_block(count=4):
    for port in range(54000, 62000, count):
        sockets = []
        try:
            for offset in range(count):
                s = socket.socket()
                s.bind(("127.0.0.1", port + offset))
                sockets.append(s)
            return port
        except OSError:
            pass
        finally:
            for s in sockets:
                s.close()
    raise RuntimeError("no free test port block")


async def eventually(predicate, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition did not become true before deadline")


@pytest_asyncio.fixture
async def cluster(tmp_path):
    inventory = make_inventory(subsystems=1, port=free_port_block())
    simulation = Simulation(inventory)
    archive = Archive(f"sqlite:///{tmp_path}/test.db")
    cpu = Controller(inventory, archive)
    await simulation.start()
    await cpu.start()
    try:
        try:
            await eventually(
                lambda: (
                    len(cpu.masters) == 1
                    and len(cpu.current) == len(inventory.nodes) * len(inventory.parameters)
                )
            )
        except AssertionError:
            print(
                "Cold-start diagnostics",
                {
                    n: {
                        "lifecycle": a.lifecycle,
                        "master": a.authoritative,
                        "preferred": a.preferred(),
                        "grant": str(a.voter.grant),
                        "peers": {p: (i.get("eligible"), i.get("role")) for p, i in a.peers.items()},
                        "tasks": [str(t.exception()) for t in a.tasks if t.done() and not t.cancelled()],
                    }
                    for n, a in simulation.agents.items()
                },
                "CPU",
                cpu.masters,
                len(cpu.current),
                cpu.last_error,
            )
            raise
        yield simulation, cpu
    finally:
        print(
            "Final cluster state",
            {
                n: {
                    "lifecycle": a.lifecycle,
                    "role": a.identity()["role"],
                    "preferred": a.preferred(),
                    "buffer": a.identity()["buffer"],
                    "errors": a.peer_errors,
                    "online": a.online,
                    "peers": {k: (v["role"], v["eligible"], v.get("buffer")) for k, v in a.peers.items()},
                    "tasks": [str(t.exception()) for t in a.tasks if t.done() and not t.cancelled()],
                }
                for n, a in simulation.agents.items()
            },
            "CPU",
            cpu.masters,
            cpu.last_error,
            "gaps",
            cpu.archive.list("gap"),
        )
        await cpu.close()
        await simulation.close()
        archive.close()
