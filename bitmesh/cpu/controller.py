from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from uuid import uuid4

import grpc

from bitmesh.models import Command, Inventory, Measurement
from bitmesh.wire import Transport, pack, unpack

log = logging.getLogger(__name__)


class Controller:
    def __init__(self, inventory: Inventory, archive, tls=None):
        self.inventory, self.archive = inventory, archive
        self.by_id = {n.id: n for n in inventory.nodes}
        self.groups = {
            s: [n for n in inventory.nodes if n.subsystem == s]
            for s in {n.subsystem for n in inventory.nodes}
        }
        self.transport = Transport(tls, timeout_s=inventory.rpc_timeout_s)
        self.masters: dict[str, dict] = {}
        self.members: dict[str, dict] = {}
        self.current: dict[str, dict] = {}
        self.received: dict[str, float] = {}
        self.latencies: list[float] = []
        self.fast_latencies: list[float] = []
        self.subscribers: set[asyncio.Queue] = set()
        self.tasks = []
        self.browser_drops = 0
        self.archive_queue = asyncio.Queue(maxsize=32)
        self.archive_drops = 0
        self.commands_lock = asyncio.Lock()
        self.pending: OrderedDict[str, dict] = OrderedDict()
        self.pending_deadlines: dict[str, float] = {}
        self.last_error: str | None = None

    async def authority_valid(self, subsystem, authority):
        members = self.groups[subsystem]
        if authority.get("candidate") not in {n.id for n in members}:
            return False

        started = time.monotonic()

        async def verify(node):
            try:
                r = await self.transport.call(node, "Peer", {"action": "verify", **authority})
                return float(r.get("valid_for_s", 0)) if r.get("valid") else 0.0
            except grpc.RpcError:
                return 0.0

        remaining = await asyncio.gather(*(verify(n) for n in members))
        elapsed = time.monotonic() - started
        return sum(value > elapsed + 0.5 for value in remaining) >= len(members) // 2 + 1

    async def discover(self, subsystem):
        async def probe(node):
            try:
                result = await self.transport.call(node, "Identity", {})
                if result["som_id"] != node.id or result["subsystem"] != subsystem:
                    return None
                await self.update_member(result)
                if result["role"] == "MASTER" and await self.authority_valid(subsystem, result["authority"]):
                    return result
            except grpc.RpcError:
                pass
            return None

        found = [r for r in await asyncio.gather(*(probe(n) for n in self.groups[subsystem])) if r]
        return found[0] if len(found) == 1 else None

    async def update_member(self, identity):
        node_id = identity["som_id"]
        previous = self.members.get(node_id)
        boot_changed = bool(previous and previous["boot_id"] != identity["boot_id"])
        if boot_changed:
            await asyncio.to_thread(
                self.archive.save,
                "gap",
                f"boot:{node_id}:{identity['boot_id']}",
                {
                    "som_id": node_id,
                    "boot_id": identity["boot_id"],
                    "description": "Boot changed: unarchived RAM history may have been lost",
                },
                node_id,
            )
            for key, measurement in list(self.current.items()):
                if measurement["som_id"] == node_id:
                    self.current.pop(key)
        self.members[node_id] = identity
        self.received[node_id] = time.monotonic() - identity.get("last_seen_age_s", 0)
        if not previous or boot_changed or previous.get("connection") != identity.get("connection"):
            await asyncio.to_thread(self.archive.save, "connectivity", str(uuid4()), identity, node_id)

    def ingest(self, batch):
        now = time.time()
        validated = []
        for data in batch:
            m = Measurement.model_validate(data)
            if m.som_id not in self.by_id:
                raise ValueError("measurement from outside inventory")
            known_boot = self.members.get(m.som_id, {}).get("boot_id")
            if known_boot and known_boot != m.boot_id:
                continue
            m.cpu_time = now
            key = f"{m.som_id}/{m.parameter.id}"
            old = self.current.get(key)
            if old and old["boot_id"] == m.boot_id and old["sequence"] >= m.sequence:
                continue
            value = m.model_dump(mode="json")
            value["received_monotonic"] = time.monotonic()
            self.current[key] = value
            validated.append(m)
            self.latencies.append(now - m.source_time)
            if m.parameter.collection_s <= 0.2 and m.quality.state == "SYNCHRONIZED":
                self.fast_latencies.append(now - m.source_time)
        if len(self.fast_latencies) > 10000:
            self.fast_latencies = self.fast_latencies[-10000:]
        if len(self.latencies) > 10000:
            self.latencies = self.latencies[-10000:]
        if validated:
            if self.archive_queue.full():
                self.archive_queue.get_nowait()
                self.archive_drops += 1
            self.archive_queue.put_nowait(validated)
            self.broadcast(
                {"type": "telemetry", "measurements": [m.model_dump(mode="json") for m in validated]}
            )

    def broadcast(self, message):
        for queue in self.subscribers:
            if queue.full():
                queue.get_nowait()
                self.browser_drops += 1
            queue.put_nowait(message)

    async def archive_worker(self):
        while True:
            batches = [await self.archive_queue.get()]
            # Coalesce ordinary history into bounded transactions; events retain
            # their separate immediate commit-before-ack path.
            await asyncio.sleep(0.025)
            while not self.archive_queue.empty() and sum(map(len, batches)) < 2048:
                batches.append(self.archive_queue.get_nowait())
            try:
                await asyncio.to_thread(self.archive.measurements, [m for batch in batches for m in batch])
            except Exception:
                self.last_error = "Measurement archival failed; inspect CPU logs"
                log.exception("Measurement archive write failed")
            finally:
                for _ in batches:
                    self.archive_queue.task_done()

    async def live(self, node):
        stream = self.transport.stub(node.endpoint).Live(pack({}))
        try:
            async for envelope in stream:
                self.ingest(unpack(envelope).get("measurements", []))
        finally:
            stream.cancel()

    async def events(self, node):
        stream = self.transport.stub(node.endpoint).Events(pack({}))
        try:
            async for envelope in stream:
                page = unpack(envelope)
                ack = await asyncio.to_thread(self.archive.ingest_events, page)
                if ack or page.get("gaps"):
                    await self.transport.call(
                        node,
                        "Acknowledge",
                        {
                            "som_id": page["som_id"],
                            "boot_id": page["boot_id"],
                            "sequence": ack,
                            "received_gaps": page.get("gaps", []),
                        },
                    )
                if page.get("events") or page.get("gaps"):
                    self.broadcast({"type": "events"})
        finally:
            stream.cancel()

    async def snapshots(self, node):
        while True:
            offset = 0
            while offset is not None:
                result = await self.transport.call(node, "Snapshot", {"offset": offset}, timeout=1)
                session = self.masters[node.subsystem]
                if (
                    result["master"]["boot_id"] != session["boot_id"]
                    or result["master"]["authority"]["generation"] != session["authority"]["generation"]
                ):
                    raise ValueError("master tenure changed; rediscover and verify new authority")
                for member in result["members"]:
                    await self.update_member(member)
                self.ingest(result["measurements"])
                offset = result.get("next_offset")
            await self.reconcile()
            await asyncio.sleep(1)

    async def subsystem_loop(self, subsystem):
        while True:
            session = []
            try:
                identity = await self.discover(subsystem)
                if not identity:
                    self.masters.pop(subsystem, None)
                    await asyncio.sleep(0.25)
                    continue
                node = self.by_id[identity["som_id"]]
                self.masters[subsystem] = identity
                await asyncio.to_thread(self.archive.save, "leadership", str(uuid4()), identity, node.id)
                session = [asyncio.create_task(f(node)) for f in (self.live, self.events, self.snapshots)]
                done, _ = await asyncio.wait(session, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    task.result()
            except (grpc.RpcError, ValueError, KeyError):
                log.debug("Subsystem session interrupted", exc_info=True)
            except Exception:
                self.last_error = f"Subsystem {subsystem} archive or session failure; inspect CPU logs"
                log.exception("Subsystem session failure")
            finally:
                for task in session:
                    task.cancel()
                await asyncio.gather(*session, return_exceptions=True)
                self.masters.pop(subsystem, None)
            await asyncio.sleep(0.1)

    def state(self, include_measurements=True):
        now = time.monotonic()
        measurements = []
        for item in self.current.values() if include_measurements else []:
            item = item.copy()
            received = item.pop("received_monotonic")
            if now - received > max(item["parameter"]["collection_s"] * 3, 1):
                item["validity"] = "STALE"
            measurements.append(item)
        members = []
        for node in self.inventory.nodes:
            item = self.members.get(
                node.id,
                {
                    "som_id": node.id,
                    "subsystem": node.subsystem,
                    "ip": str(node.ip),
                    "boot_id": "",
                    "lifecycle": "BOOTING",
                    "role": "FOLLOWER",
                },
            ).copy()
            if now - self.received.get(node.id, -100) > 2:
                item["connection"] = "DISCONNECTED"
            members.append(item)
        return {
            "members": members,
            "stale_keys": [
                key
                for key, item in self.current.items()
                if now - item["received_monotonic"] > max(item["parameter"]["collection_s"] * 3, 1)
            ],
            "masters": self.masters,
            "measurements": measurements,
            "cpu_time": time.time(),
            "error": self.last_error,
            "metrics": {
                "browser_drops": self.browser_drops,
                "archive_drops": self.archive_drops,
                "source_to_cpu_p95_ms": sorted(self.latencies)[int(len(self.latencies) * 0.95)] * 1000
                if self.latencies
                else None,
            },
        }

    async def dispatch(self, command: Command, role: str):
        async with self.commands_lock:
            if (
                role not in {"operator", "admin"}
                or command.kind
                in {"approve", "reset", "program_pl", "update_software", "transfer", "configure"}
                and role != "admin"
            ):
                raise PermissionError("operator/admin authorization required")
            previous = await asyncio.to_thread(self.archive.get, "command", command.id)
            if previous:
                if any(
                    previous[k] != command.model_dump(mode="json")[k]
                    for k in ("target", "boot_id", "kind", "operator", "arguments")
                ):
                    raise ValueError("command ID already used for a different operation")
                return previous
            if (
                role not in {"operator", "admin"}
                or command.kind
                in {"approve", "reset", "program_pl", "update_software", "transfer", "configure"}
                and role != "admin"
            ):
                raise PermissionError("operator/admin authorization required")
            if command.target not in self.by_id:
                raise ValueError("unknown target")
            if command.boot_id != self.members.get(command.target, {}).get("boot_id"):
                raise ValueError("boot identity changed; refresh target")
            node = self.by_id[command.target]
            master = self.masters.get(node.subsystem)
            authority = master["authority"] if master else {}
            fresh = None
            if master:
                try:
                    fresh = await self.transport.call(self.by_id[master["som_id"]], "Identity", {})
                except grpc.RpcError:
                    pass
            verified_session = bool(
                fresh
                and fresh["role"] == "MASTER"
                and fresh["boot_id"] == master["boot_id"]
                and fresh["authority"]["generation"] == authority["generation"]
            )
            if verified_session:
                authority = fresh["authority"]
            else:
                if command.kind != "approve":
                    raise ValueError("exclusive master authority unavailable")
                # Admin recovery approval is CPU-authorized and boot-scoped, never
                # a processing command. It breaks an all-quarantined startup deadlock.
                master = {"som_id": node.id}
                authority = {}
            if command.target == master["som_id"] and command.kind in {
                "reset",
                "program_pl",
                "update_software",
            }:
                raise ValueError("transfer master and verify CPU reconnection before maintenance")
            command.validation = "VALIDATED"
            command.state = "ACCEPTED"
            local_deadline = time.monotonic() + command.timeout_s
            await asyncio.to_thread(
                self.archive.save, "command", command.id, command.model_dump(mode="json"), command.target
            )
            await asyncio.to_thread(
                self.archive.save,
                "audit",
                str(uuid4()),
                {
                    "operator": command.operator,
                    "command": command.id,
                    "kind": command.kind,
                    "time": time.time(),
                },
                command.target,
            )
            if command.kind == "approve":
                await asyncio.to_thread(
                    self.archive.save, "approval", command.id, command.model_dump(mode="json"), command.target
                )
            if command.kind == "configure":
                await asyncio.to_thread(
                    self.archive.save,
                    "configuration",
                    command.id,
                    command.model_dump(mode="json"),
                    command.target,
                )
            try:
                result = await self.transport.call(
                    self.by_id[master["som_id"]],
                    "Command",
                    {"command": command.model_dump(mode="json"), "authority": authority},
                    timeout=command.timeout_s,
                )
            except grpc.RpcError as exc:
                command.state = "UNKNOWN"
                command.error = f"Delivery uncertain: {exc.code().name}; reconcile before retry"
                result = command.model_dump(mode="json")
            await asyncio.to_thread(self.archive.save, "command", command.id, result, command.target)
            if result["state"] == "SUCCEEDED" and command.kind == "configure":
                allowed = {p["id"] for p in command.arguments["parameters"]}
                for key, item in list(self.current.items()):
                    if item["som_id"] == command.target and item["parameter"]["id"] not in allowed:
                        self.current.pop(key)
            if result["state"] == "UNKNOWN":
                self.pending_deadlines[command.id] = local_deadline
                self.pending[command.id] = result
                while len(self.pending) > 1000:
                    evicted, _ = self.pending.popitem(last=False)
                    self.pending_deadlines.pop(evicted, None)
            self.broadcast({"type": "command", "command": result})
            return result

    async def reconcile(self):
        for key, command in list(self.pending.items()):
            target = command["target"]
            member = self.members.get(target, {})
            success = False
            if command["kind"] == "reset":
                success = bool(member.get("boot_id") and member["boot_id"] != command["boot_id"])
            elif command["kind"] in {"start", "restart", "stop", "approve"}:
                expected = "ACTIVE" if command["kind"] in {"start", "restart"} else "READY"
                success = (
                    member.get("lifecycle") == expected
                    and member.get("boot_id") == command["boot_id"]
                    and self.received.get(target, 0) > 0
                    and any(
                        receipt.get("id") == command["id"]
                        and receipt.get("kind") == command["kind"]
                        and receipt.get("boot_id") == command["boot_id"]
                        for receipt in member.get("operation_receipts", [])
                    )
                )
            elif command["kind"] == "transfer":
                subsystem = self.by_id[target].subsystem
                success = self.masters.get(subsystem, {}).get("som_id") == command["arguments"].get(
                    "successor"
                )
            elif command["kind"] in {"program_pl", "update_software"}:
                parameter = "pl.version" if command["kind"] == "program_pl" else "software.version"
                value = self.current.get(f"{target}/{parameter}", {})
                success = (
                    value.get("value") == command["arguments"].get("artifact")
                    and any(
                        receipt.get("id") == command["id"]
                        and receipt.get("kind") == command["kind"]
                        and receipt.get("boot_id") == command["boot_id"]
                        for receipt in member.get("operation_receipts", [])
                    )
                    and member.get("lifecycle") == "READY"
                    and member.get("connection") == "CONNECTED"
                )
            if success or time.monotonic() > self.pending_deadlines.get(key, 0):
                command["state"] = "SUCCEEDED" if success else "UNKNOWN"
                command["error"] = None if success else "Reconciliation deadline elapsed; outcome unknown"
                command["completed_at"] = time.time()
                await asyncio.to_thread(self.archive.save, "command", key, command, target)
                self.pending.pop(key, None)
                self.pending_deadlines.pop(key, None)

    async def start(self):
        for identity in await asyncio.to_thread(self.archive.latest_members):
            if identity["som_id"] in self.by_id:
                self.members.setdefault(identity["som_id"], identity)
        for node in self.inventory.nodes:
            await asyncio.to_thread(
                self.archive.save, "inventory", node.id, node.model_dump(mode="json"), node.id
            )
        for command in await asyncio.to_thread(self.archive.list, "command", limit=10000):
            if command["state"] in {"ACCEPTED", "EXECUTING", "UNKNOWN"} and not command.get("completed_at"):
                self.pending[command["id"]] = command
                # Monotonic values cannot survive CPU restart. Reconstruct the
                # remaining UTC budget once, then keep scheduling monotonic.
                self.pending_deadlines.setdefault(
                    command["id"],
                    time.monotonic() + max(0.0, command["requested_at"] + command["timeout_s"] - time.time()),
                )
        self.tasks = [asyncio.create_task(self.subsystem_loop(s)) for s in self.groups]
        self.tasks.append(asyncio.create_task(self.archive_worker()))
        self.tasks.append(asyncio.create_task(self.retention_loop()))
        self.tasks.append(asyncio.create_task(self.browser_state_loop()))

    async def browser_state_loop(self):
        while True:
            if self.subscribers:
                self.broadcast({"type": "membership", "state": self.state(include_measurements=False)})
            await asyncio.sleep(1)

    async def retention_loop(self):
        while True:
            await asyncio.to_thread(self.archive.prune)
            await asyncio.sleep(60)

    async def close(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.transport.close()
