import asyncio
import time
from pathlib import Path

import yaml

from bitmesh.agent.providers import MockProvider, default_parameters
from bitmesh.agent.service import Agent
from bitmesh.models import Inventory, Lifecycle, Node, Quality


def make_inventory(subsystems=3, soms=4, parameters=12, port=51000):
    nodes = [
        Node(
            id=f"s{s:02d}-n{n:02d}",
            subsystem=f"S{s:02d}",
            ip=f"10.40.{s}.{n + 10}",
            endpoint=f"127.0.0.1:{port + (s - 1) * soms + n - 1}",
        )
        for s in range(1, subsystems + 1)
        for n in range(1, soms + 1)
    ]
    return Inventory(nodes=nodes, parameters=default_parameters(parameters))


def load_inventory(path):
    return Inventory.model_validate(yaml.safe_load(Path(path).read_text()))


class Simulation:
    def __init__(self, inventory):
        self.inventory = inventory
        self.agents = {
            n.id: Agent(n, inventory, MockProvider(i), bootstrap=True, deferred_bootstrap=True)
            for i, n in enumerate(inventory.nodes)
        }

    async def start(self):
        await asyncio.gather(*(agent.start() for agent in self.agents.values()))
        if not getattr(self, "defer_activation", False):
            self.activate()

    def activate(self):
        for agent in self.agents.values():
            if agent.lifecycle == Lifecycle.BOOTING:
                agent.set_lifecycle(Lifecycle.READY)

    async def close(self):
        await asyncio.gather(*(agent.close() for agent in self.agents.values()))

    async def inject(self, target, action, value=None):
        if target not in self.agents:
            raise ValueError("unknown simulated SOM")
        a = self.agents[target]
        if action in {"temperature", "cpu", "memory", "interface"}:
            key = {
                "temperature": "ps.temperature",
                "cpu": "cpu.utilization",
                "memory": "memory.utilization",
                "interface": "ethernet.errors",
            }[action]
            a.provider.overrides[key] = int(value) if action == "interface" else float(value)
        elif action in {"disconnect", "master_failure"}:
            a.online = False
            a.lease_until = 0
            a.set_lifecycle(Lifecycle.FAILED)
        elif action == "quarantine":
            a.set_lifecycle(Lifecycle.QUARANTINED)
            a.lease_until = 0
        elif action == "reconnect":
            a.online = True
            if a.lifecycle == Lifecycle.FAILED:
                a.set_lifecycle(Lifecycle.QUARANTINED)
        elif action == "reboot":
            if a.lifecycle not in {Lifecycle.READY, Lifecycle.ACTIVE}:
                raise ValueError("reboot injection requires running SOM")
            a.set_lifecycle(Lifecycle.MAINTENANCE)
            await a.outage("reset")
        elif action == "overflow":
            # Shrink the same journal: overflow must never reuse sequence IDs
            # within a boot session or silently discard pre-existing events.
            a.journal.budget = 4096
            for i in range(int(value or 100)):
                a.event("injected", f"Injected fault {i}", "CRITICAL", state="ACTIVE")
        elif action == "ptp":
            state = str(value)
            a.provider.quality = Quality(
                state=state,
                uncertainty_us=20 if state == "SYNCHRONIZED" else 1000,
                offset_us=0 if state == "SYNCHRONIZED" else 500,
            )
            a.event("time", f"PTP state {state}", "WARNING" if state != "SYNCHRONIZED" else "INFO")
        else:
            raise ValueError("unknown injection")
        return a.identity()


def _subsystem_worker(inventory_data, subsystem, pipe):
    """Spawn entry point: process isolation models separate subsystem CPUs."""
    import traceback

    async def run():
        inventory = Inventory.model_validate(inventory_data)
        agents = {
            n.id: Agent(n, inventory, MockProvider(i), bootstrap=True, deferred_bootstrap=True)
            for i, n in enumerate(inventory.nodes)
            if n.subsystem == subsystem
        }
        sim = Simulation.__new__(Simulation)
        sim.inventory, sim.agents = inventory, agents
        sim.defer_activation = True
        try:
            await sim.start()
            pipe.send({"ready": True})
            while True:
                if pipe.poll():
                    request = pipe.recv()
                    if request["action"] == "close":
                        break
                    try:
                        if request["action"] == "activate":
                            sim.activate()
                            result = {"activated": len(agents)}
                        elif request["action"] == "stats":
                            result = {
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
                                for n, a in agents.items()
                            }
                        else:
                            result = await sim.inject(
                                request["target"], request["action"], request.get("value")
                            )
                        pipe.send({"result": result})
                    except Exception as exc:
                        pipe.send({"error": str(exc)})
                await asyncio.sleep(0.02)
        except Exception:
            pipe.send({"error": traceback.format_exc()})
        finally:
            await sim.close()
            pipe.close()

    asyncio.run(run())


class ProcessSimulation:
    """One process per subsystem. Injections only alter providers/links, never CPU state."""

    def __init__(self, inventory):
        import multiprocessing
        import threading

        self.inventory = inventory
        self.context = multiprocessing.get_context("spawn")
        self.workers = {}
        self.locks = {}
        self.thread_lock_type = threading.Lock

    async def start(self):
        # Stagger process boot so 400 simultaneous agent initializations do not
        # create a synthetic full-mesh startup storm on the development host.
        deadline = time.monotonic() + 60
        for subsystem in sorted({n.subsystem for n in self.inventory.nodes}):
            if time.monotonic() >= deadline:
                raise TimeoutError("subsystem worker startup exceeded 60 s; reduce load or add host CPU")
            parent, child = self.context.Pipe()
            process = self.context.Process(
                target=_subsystem_worker,
                args=(self.inventory.model_dump(mode="json"), subsystem, child),
                daemon=True,
            )
            process.start()
            child.close()
            self.workers[subsystem] = (process, parent)
            self.locks[subsystem] = self.thread_lock_type()

            def receive(pipe=parent, name=subsystem):
                if not pipe.poll(max(0.1, deadline - time.monotonic())):
                    raise TimeoutError(f"{name} worker startup timed out")
                result = pipe.recv()
                if not result.get("ready"):
                    raise RuntimeError(result)

            await asyncio.to_thread(receive)
            await asyncio.sleep(0.1)
        # A boot barrier avoids treating not-yet-created simulation workers as
        # failed operational peers. No telemetry or archive path is bypassed.
        for subsystem in self.workers:
            await self.request(subsystem, {"action": "activate"})
            await asyncio.sleep(0.1)

    async def request(self, subsystem, payload):
        def exchange():
            _, pipe = self.workers[subsystem]
            with self.locks[subsystem]:
                pipe.send(payload)
                if not pipe.poll(10):
                    raise TimeoutError("simulation control request timed out")
                result = pipe.recv()
                if "error" in result:
                    raise ValueError(result["error"])
                return result["result"]

        return await asyncio.to_thread(exchange)

    async def inject(self, target, action, value=None):
        node = next((n for n in self.inventory.nodes if n.id == target), None)
        if node is None:
            raise ValueError("unknown simulated SOM")
        return await self.request(node.subsystem, {"target": target, "action": action, "value": value})

    async def stats(self):
        groups = await asyncio.gather(*(self.request(s, {"action": "stats"}) for s in self.workers))
        return {k: v for group in groups for k, v in group.items()}

    async def close(self):
        async def stop(subsystem):
            process, pipe = self.workers[subsystem]
            try:
                pipe.send({"action": "close"})
                await asyncio.to_thread(process.join, 5)
            finally:
                if process.is_alive():
                    process.terminate()
                    await asyncio.to_thread(process.join, 3)
                pipe.close()

        await asyncio.gather(*(stop(s) for s in self.workers))
