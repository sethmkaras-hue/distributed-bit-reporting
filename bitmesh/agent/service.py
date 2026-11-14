"""Identical agent for hardware and simulation; all external operations cross gRPC."""

from __future__ import annotations

import asyncio
import hashlib
import json
import ipaddress
import time
from collections import OrderedDict, deque
from pathlib import Path
from uuid import uuid4

import grpc

from bitmesh import mesh_pb2_grpc as rpc
from bitmesh.agent.election import Voter
from bitmesh.agent.journal import Cache, Journal
from bitmesh.agent.providers import Provider, default_parameters
from bitmesh.models import Command, Inventory, Lifecycle, Measurement, Node, Parameter, transition
from bitmesh.wire import MAX_WIRE, Transport, pack, unpack


class Agent(rpc.AgentServicer):
    def __init__(
        self,
        node: Node,
        inventory: Inventory,
        provider: Provider,
        *,
        bootstrap=False,
        deferred_bootstrap=False,
        tls=None,
    ):
        self.node, self.inventory, self.provider = node, inventory, provider
        self.members = sorted(
            [n for n in inventory.nodes if n.subsystem == node.subsystem], key=lambda n: int(n.ip)
        )
        self.by_id = {n.id: n for n in self.members}
        self.boot_id = str(uuid4())
        self.lifecycle = (
            Lifecycle.BOOTING
            if deferred_bootstrap
            else (Lifecycle.READY if bootstrap else Lifecycle.QUARANTINED)
        )
        self.peer_failure_s = max(2.0, inventory.peer_heartbeat_s * 2 + inventory.rpc_timeout_s)
        self.online = True
        self.pl_unavailable = False
        self.operation_receipts = deque(maxlen=16)
        self.voter = Voter(inventory.lease_s)
        self.generation = str(uuid4())
        self.had_authority = False
        self.lease_until = 0.0
        self.grantors: list[str] = []
        self.peers: dict[str, dict] = {}
        self.peer_errors: dict[str, str] = {}
        self.seen: dict[str, float] = {}
        self.master: dict | None = None
        self.transport = Transport(tls, timeout_s=inventory.rpc_timeout_s)
        self.tls = tls
        budget = inventory.memory_mb * 1024 * 1024
        self.journal = Journal(budget // 2)
        self.cache = Cache(budget // 4)
        self.aggregate: OrderedDict[tuple, dict] = OrderedDict()
        self.aggregate_bytes = 0
        self.aggregate_budget = budget // 8
        self.commands: OrderedDict[str, tuple[str, dict]] = OrderedDict()
        self.command_bytes = 0
        self.command_budget = budget // 32
        self.live: set[asyncio.Queue] = set()
        self.queue_drops = 0
        self.parameters = [p.model_copy(deep=True) for p in inventory.parameters] or default_parameters()
        self.collected: dict[str, float] = {}
        self.published: dict[str, float] = {}
        self.last_values: dict[str, object] = {}
        self.outbox: dict[str, dict] = {}
        self.last_full_forward = 0.0
        self.last_mesh_heartbeat = 0.0
        self.failed_peers: set[str] = set()
        self.severities: dict[str, str] = {}
        self.measurement_sequence = 0
        self.tasks: list[asyncio.Task] = []
        self.operation_tasks: set[asyncio.Task] = set()
        self.server = None
        self.lock = asyncio.Lock()
        self.transfer_hold_until = 0.0
        self.preferred_transfer: str | None = None
        self.event("lifecycle", "Agent boot; RAM history starts a new session", "INFO")

    @property
    def eligible(self):
        return self.online and self.lifecycle in {Lifecycle.READY, Lifecycle.ACTIVE}

    @property
    def authoritative(self):
        return self.eligible and time.monotonic() < self.lease_until

    def event(self, category, description, severity="WARNING", parameter_id=None, state="NOTICE"):
        return self.journal.append(
            som_id=self.node.id,
            boot_id=self.boot_id,
            severity=severity,
            category=category,
            description=description,
            parameter_id=parameter_id,
            state=state,
            quality=self.provider.quality.model_copy(),
        )

    def set_lifecycle(self, new, approved=False):
        old = self.lifecycle
        self.lifecycle = transition(old, new, approved=approved)
        self.event("lifecycle", f"{old} -> {new}", "INFO")

    def identity(self):
        return {
            "som_id": self.node.id,
            "subsystem": self.node.subsystem,
            "ip": str(self.node.ip),
            "boot_id": self.boot_id,
            "lifecycle": self.lifecycle,
            "eligible": self.eligible,
            "connection": "CONNECTED" if self.online else "DISCONNECTED",
            "role": "MASTER" if self.authoritative else "FOLLOWER",
            "authority": {
                "candidate": self.node.id,
                "boot_id": self.boot_id,
                "generation": self.generation,
                "grantors": self.grantors,
                "valid_for_s": max(0.0, self.lease_until - time.monotonic()),
            },
            "quality": self.provider.quality.model_dump(),
            "pl_unavailable": self.pl_unavailable,
            "operation_receipts": list(self.operation_receipts),
            "peer_errors": self.peer_errors.copy(),
            "buffer": {
                "journal_bytes": self.journal.bytes,
                "event_sequence": self.journal.sequence,
                "archive_ack": self.journal.acknowledged,
                "gap_pending": bool(self.journal.gaps),
                "cache_bytes": self.cache.bytes,
                "aggregate_bytes": self.aggregate_bytes,
                "overwritten": self.journal.overwritten,
                "queue_drops": self.queue_drops,
            },
        }

    def preferred(self):
        now = time.monotonic()
        if self.preferred_transfer and now < self.transfer_hold_until:
            return self.preferred_transfer
        eligible = [self.node] if self.eligible and now >= self.transfer_hold_until else []
        eligible += [
            n
            for n in self.members
            if n.id != self.node.id
            and self.peers.get(n.id, {}).get("eligible")
            and now - self.seen.get(n.id, 0) < self.peer_failure_s
        ]
        return min(eligible, key=lambda n: int(ipaddress.ip_address(n.ip))).id if eligible else None

    async def verify(self, authority: dict) -> bool:
        candidate = authority.get("candidate")
        if (
            candidate not in self.by_id
            or len(set(authority.get("grantors", []))) < len(self.members) // 2 + 1
        ):
            return False

        started = time.monotonic()

        async def check(n):
            try:
                if n.id == self.node.id:
                    valid = self.voter.verify(candidate, authority["boot_id"], authority["generation"])
                    return max(0.0, self.voter.grant.until - time.monotonic()) if valid else 0.0
                r = await self.transport.call(n, "Peer", {"action": "verify", **authority})
                return float(r.get("valid_for_s", 0)) if r.get("valid") else 0.0
            except (grpc.RpcError, KeyError, ValueError):
                return 0.0

        checks = await asyncio.gather(*(check(n) for n in self.members))
        elapsed = time.monotonic() - started
        return sum(remaining > elapsed + 0.5 for remaining in checks) >= len(self.members) // 2 + 1

    async def authorize(self, context, allowed):
        if not self.tls:
            return
        names = context.auth_context().get("x509_common_name", [])
        if len(names) != 1 or names[0].decode() not in allowed:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "certificate identity not authorized")

    def check_local_recovery(self):
        # Network observations are not votes to quarantine another healthy node.
        # A previously connected agent returning from total peer isolation or a
        # scheduling hang must seek approval; fresh boot already starts quarantined.
        if (
            self.lifecycle in {Lifecycle.READY, Lifecycle.ACTIVE}
            and self.seen
            and time.monotonic() - max(self.seen.values()) > self.peer_failure_s
        ):
            self.set_lifecycle(Lifecycle.QUARANTINED)
            self.lease_until = 0
            self.event(
                "recovery", "Returning from total peer isolation; operator approval required", "WARNING"
            )

    async def available(self, context):
        self.check_local_recovery()
        if not self.online:
            await context.abort(grpc.StatusCode.UNAVAILABLE, "PL/control link unavailable")

    async def Peer(self, request, context):
        await self.available(context)
        data = unpack(request)
        action = data.get("action")
        await self.authorize(context, set(self.by_id) | {"cpu"})
        if action == "heartbeat":
            identity = data.get("identity", {})
            who = identity.get("som_id")
            if who not in self.by_id:
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, "unknown inventory identity")
            await self.authorize(context, {who})
            self.peers[who] = identity
            self.seen[who] = time.monotonic()
            if self.authoritative:
                for m in data.get("measurements", [])[:512]:
                    self.ingest(Measurement.model_validate(m))
            return pack({"identity": self.identity()})
        if action == "vote":
            await self.authorize(context, {data.get("candidate")})
            if data.get("candidate") not in self.by_id:
                return pack({"granted": False})
            return pack(
                self.voter.vote(data["candidate"], data["boot_id"], data["generation"], self.preferred())
            )
        if action == "verify":
            valid = self.voter.verify(data["candidate"], data["boot_id"], data["generation"])
            remaining = max(0.0, self.voter.grant.until - time.monotonic()) if valid else 0.0
            return pack({"valid": valid, "valid_for_s": remaining})
        if action == "transfer":
            await self.authorize(context, {data.get("authority", {}).get("candidate")})
            if not await self.verify(data.get("authority", {})):
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, "invalid transfer authority")
            self.preferred_transfer = data["successor"]
            self.transfer_hold_until = time.monotonic() + self.inventory.lease_s + 2
            return pack({"accepted": True})
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "unknown peer operation")

    def ingest(self, measurement: Measurement):
        measurement.master_time = time.time()
        data = measurement.model_dump(mode="json")
        key = (measurement.som_id, measurement.parameter.id)
        size = len(measurement.model_dump_json().encode()) + 128
        old = self.aggregate.pop(key, None)
        if old:
            self.aggregate_bytes -= old[1]
        while self.aggregate and self.aggregate_bytes + size > self.aggregate_budget:
            _, (_, cost) = self.aggregate.popitem(last=False)
            self.aggregate_bytes -= cost
        if size <= self.aggregate_budget:
            self.aggregate[key] = (data, size)
            self.aggregate_bytes += size
        for queue in self.live:
            if queue.full():
                queue.get_nowait()
                self.queue_drops += 1
            queue.put_nowait(data)

    async def Identity(self, request, context):
        await self.available(context)
        unpack(request)
        await self.authorize(context, {"cpu"} | set(self.by_id))
        return pack(self.identity())

    async def require_master(self, context):
        await self.available(context)
        if not self.authoritative:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "no valid master lease")

    async def Snapshot(self, request, context):
        await self.require_master(context)
        await self.authorize(context, {"cpu"})
        data = unpack(request)
        members = [self.identity()] + [p.copy() for p in self.peers.values()]
        for member in members:
            member["last_seen_age_s"] = (
                0
                if member["som_id"] == self.node.id
                else max(0, time.monotonic() - self.seen.get(member["som_id"], 0))
            )
            if (
                member["som_id"] != self.node.id
                and time.monotonic() - self.seen.get(member["som_id"], 0) > self.peer_failure_s
            ):
                member["connection"] = "DISCONNECTED"
        offset = max(0, int(data.get("offset", 0)))
        values = [self.aggregate[key] for key in sorted(self.aggregate)]
        return pack(
            {
                "master": self.identity(),
                "members": members,
                "measurements": [x[0] for x in values[offset : offset + 512]],
                "next_offset": offset + 512 if offset + 512 < len(values) else None,
            }
        )

    async def Live(self, request, context):
        await self.require_master(context)
        unpack(request)
        if len(self.live) >= 2:
            await context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "live consumer limit")
        await self.authorize(context, {"cpu"})
        queue = asyncio.Queue(maxsize=512)
        self.live.add(queue)
        try:
            while self.authoritative and self.online:
                try:
                    batch = [await asyncio.wait_for(queue.get(), 0.5)]
                    while not queue.empty() and len(batch) < 128:
                        batch.append(queue.get_nowait())
                    yield pack({"measurements": batch})
                except TimeoutError:
                    yield pack({"heartbeat": time.time()})
        finally:
            self.live.discard(queue)

    async def Replay(self, request, context):
        await self.available(context)
        data = unpack(request)
        await self.authorize(context, set(self.by_id))
        return pack(
            {
                **self.journal.replay(int(data.get("after", 0))),
                "som_id": self.node.id,
                "boot_id": self.boot_id,
            }
        )

    async def Events(self, request, context):
        await self.require_master(context)
        unpack(request)
        await self.authorize(context, {"cpu"})
        while self.authoritative and self.online:
            for node in self.members:
                try:
                    if node.id == self.node.id:
                        page = {
                            **self.journal.replay(self.journal.acknowledged),
                            "som_id": node.id,
                            "boot_id": self.boot_id,
                        }
                    else:
                        page = await self.transport.call(node, "Replay", {})
                    yield pack(page)
                except grpc.RpcError:
                    continue
            await asyncio.sleep(0.15)

    async def Acknowledge(self, request, context):
        await self.available(context)
        data = unpack(request)
        await self.authorize(context, {"cpu"} | set(self.by_id))
        if data["som_id"] != self.node.id:
            await self.require_master(context)
            return pack(await self.transport.call(self.by_id[data["som_id"]], "Acknowledge", data))
        if data["boot_id"] != self.boot_id:
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "stale boot acknowledgment")
        self.journal.ack(int(data["sequence"]))
        for gap in data.get("received_gaps", []):
            gap = tuple(gap)
            if gap in self.journal.gaps:
                self.journal.gaps.remove(gap)
        return pack({"acknowledged": self.journal.acknowledged})

    async def Configure(self, request, context):
        # Configuration is a fenced, audited command, never an unauthenticated side channel.
        return await self.Command(request, context)

    async def Command(self, request, context):
        await self.available(context)
        data = unpack(request)
        command = Command.model_validate(data["command"])
        # Convert UTC request age once at ingress; all local elapsed-time
        # checks then use monotonic time, including time spent awaiting locks.
        remaining = context.time_remaining()
        budget = min(command.timeout_s, remaining if remaining is not None else command.timeout_s)
        age = max(0.0, time.time() - command.requested_at)
        local_deadline = time.monotonic() + max(0.0, min(budget, command.timeout_s - age))
        await self.authorize(context, {"cpu", data.get("authority", {}).get("candidate")})
        bootstrap_approval = command.kind == "approve" and not data.get("authority")
        if (
            not bootstrap_approval
            and data.get("authority", {}).get("candidate") == self.node.id
            and not self.authoritative
        ):
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "candidate is no longer authoritative")
        if bootstrap_approval:
            await self.authorize(context, {"cpu"})
            if command.target != self.node.id:
                await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "approval must target this identity")
        elif not await self.verify(data.get("authority", {})):
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "leadership authority rejected")
        if command.target != self.node.id:
            await self.require_master(context)
            return pack(
                await self.transport.call(
                    self.by_id[command.target], "Command", data, timeout=min(command.timeout_s, 3)
                )
            )
        async with self.lock:
            if not bootstrap_approval and not await self.verify(data.get("authority", {})):
                await context.abort(
                    grpc.StatusCode.PERMISSION_DENIED, "authority expired while waiting for command lock"
                )
            fingerprint = hashlib.sha256(command.model_dump_json().encode()).hexdigest()
            if command.id in self.commands:
                previous, result = self.commands[command.id]
                if previous != fingerprint:
                    await context.abort(
                        grpc.StatusCode.ALREADY_EXISTS, "command ID reused with different payload"
                    )
                return pack(result)
            command.validation = "VALIDATED"
            if command.boot_id != self.boot_id:
                command.state, command.error = "FAILED", "stale boot identity"
            elif time.monotonic() > local_deadline:
                command.state, command.error = "TIMED_OUT", "deadline expired"
            elif self.lifecycle not in {Lifecycle.READY, Lifecycle.ACTIVE} and command.kind not in {
                "status",
                "approve",
            }:
                command.state, command.error = "FAILED", f"operational commands forbidden in {self.lifecycle}"
            else:
                try:
                    await self.execute(command)
                except (ValueError, NotImplementedError) as exc:
                    command.state, command.error = "FAILED", str(exc)
            if command.state not in {"UNKNOWN", "EXECUTING"}:
                command.completed_at = time.time()
            if command.state == "SUCCEEDED" and command.kind != "status":
                self.operation_receipts.append(
                    {"id": command.id, "kind": command.kind, "boot_id": self.boot_id}
                )
            result = command.model_dump(mode="json")
            cost = len(json.dumps(result).encode()) + 256
            if cost <= self.command_budget:
                while self.commands and (
                    len(self.commands) >= 256 or self.command_bytes + cost > self.command_budget
                ):
                    _, (_, old) = self.commands.popitem(last=False)
                    self.command_bytes -= len(json.dumps(old).encode()) + 256
                self.commands[command.id] = (fingerprint, result)
                self.command_bytes += cost
            self.event("command", f"{command.kind}: {command.state}; operator={command.operator}", "INFO")
            return pack(result)

    async def execute(self, command: Command):
        kind = command.kind
        command.state = "EXECUTING"
        if kind == "approve":
            await self.heartbeat(force_mesh=True)
            self.set_lifecycle(Lifecycle.READY, approved=True)
        elif kind == "start":
            if self.lifecycle != Lifecycle.ACTIVE:
                await self.provider.execute(kind, command.arguments)
                self.set_lifecycle(Lifecycle.ACTIVE)
        elif kind in {"stop", "restart"}:
            await self.provider.execute(kind, command.arguments)
            if self.lifecycle == Lifecycle.ACTIVE:
                self.set_lifecycle(Lifecycle.READY)
            if kind == "restart":
                self.set_lifecycle(Lifecycle.ACTIVE)
        elif kind == "configure":
            raw_parameters = command.arguments.get("parameters", [])
            if not isinstance(raw_parameters, list):
                raise ValueError("parameters must be a list")
            parameters = [Parameter.model_validate(p) for p in raw_parameters]
            if not 1 <= len(parameters) <= 500 or len({p.id for p in parameters}) != len(parameters):
                raise ValueError("configuration requires 1–500 uniquely identified parameters")
            self.parameters = parameters
            self.cache = Cache(self.cache.budget)
            self.outbox.clear()
            self.last_values.clear()
            self.severities.clear()
            self.collected.clear()
            self.published.clear()
        elif kind == "transfer":
            successor = command.arguments.get("successor")
            if not self.authoritative or successor == self.node.id or successor not in self.by_id:
                raise ValueError("eligible distinct successor required")
            peer = self.peers.get(successor, {})
            if (
                not peer.get("eligible")
                or time.monotonic() - self.seen.get(successor, 0) > self.peer_failure_s
            ):
                raise ValueError("successor unavailable")
            authority = self.identity()["authority"]
            results = await asyncio.gather(
                *(
                    self.transport.call(
                        n, "Peer", {"action": "transfer", "authority": authority, "successor": successor}
                    )
                    for n in self.members
                    if n.id != self.node.id
                ),
                return_exceptions=True,
            )
            if (
                sum(isinstance(r, dict) and r.get("accepted", False) for r in results)
                < len(self.members) // 2
            ):
                raise ValueError("transfer did not reach quorum")
            self.preferred_transfer = successor
            self.transfer_hold_until = time.monotonic() + self.inventory.lease_s + 2
            self.lease_until = 0
            command.state = "UNKNOWN"
            return
        elif kind in {"program_pl", "reset", "update_software"}:
            if self.authoritative:
                raise ValueError("transfer leadership and verify CPU reconnection before master maintenance")
            await self.provider.execute(kind, command.arguments)
            self.set_lifecycle(Lifecycle.MAINTENANCE)
            command.state = "UNKNOWN"
            task = asyncio.create_task(self.outage(kind, command.id))
            self.operation_tasks.add(task)
            task.add_done_callback(self.operation_tasks.discard)
            return
        elif kind != "status":
            raise ValueError("unsupported command")
        command.state = "SUCCEEDED"

    async def outage(self, kind, command_id=None):
        await self.heartbeat(force_mesh=True)  # announce maintenance before removing the control link
        await asyncio.sleep(0.05)  # allow UNKNOWN response to leave the old network session
        self.pl_unavailable = kind == "program_pl"
        self.online = False
        self.lease_until = 0
        await asyncio.sleep(1)
        if kind == "reset":
            self.boot_id = str(uuid4())
            self.journal = Journal(self.journal.budget)
            self.cache = Cache(self.cache.budget)
            self.voter = Voter(self.inventory.lease_s)
            self.generation = str(uuid4())
            self.collected.clear()
            self.published.clear()
            self.set_lifecycle(Lifecycle.QUARANTINED)
            self.event("history_gap", "Reboot destroyed unarchived volatile history", "WARNING")
        else:
            self.set_lifecycle(Lifecycle.READY)
        self.online = True
        self.pl_unavailable = False
        if command_id and kind != "reset":
            self.operation_receipts.append({"id": command_id, "kind": kind, "boot_id": self.boot_id})

    async def sample_once(self):
        now = time.monotonic()
        for p in self.parameters:
            if now - self.collected.get(p.id, -1e10) < p.collection_s:
                continue
            self.collected[p.id] = now
            unavailable = self.pl_unavailable and (p.category == "PL" or p.id == "pl.version")
            previous_measurement = self.cache.current.get(p.id)
            provider_fault = False
            try:
                value = None if unavailable else self.provider.read(p)
            except (NotImplementedError, OSError, ValueError, TypeError):
                value, unavailable, provider_fault = None, True, True
            self.measurement_sequence += 1
            fields = dict(
                som_id=self.node.id,
                boot_id=self.boot_id,
                parameter=p,
                quality=self.provider.quality.model_copy(),
                sequence=self.measurement_sequence,
            )
            try:
                m = Measurement(**fields, value=value, validity="UNAVAILABLE" if unavailable else "VALID")
            except ValueError:
                value, unavailable, provider_fault = None, True, True
                m = Measurement(**fields, value=None, validity="UNAVAILABLE")
            if provider_fault and (
                not previous_measurement or previous_measurement[0].validity != "UNAVAILABLE"
            ):
                self.event("provider", f"Provider unavailable or invalid type for {p.id}", "WARNING", p.id)
            previous = self.severities.get(p.id, "INFO")
            severity = previous if unavailable else "INFO"
            if not unavailable and type(value) in {float, int}:
                severity = (
                    "CRITICAL"
                    if p.critical is not None and value >= p.critical
                    else ("WARNING" if p.warning is not None and value >= p.warning else "INFO")
                )
            if severity != previous:
                self.event(
                    "threshold",
                    f"{p.name}: {value} {p.unit} ({severity})",
                    severity,
                    p.id,
                    "CLEARED" if severity == "INFO" else "ACTIVE",
                )
            self.severities[p.id] = severity
            m.severity = severity
            self.cache.put(m)
            old = self.last_values.get(p.id)
            changed = old != value
            if type(old) in {float, int} and type(value) in {float, int}:
                changed = abs(old - value) > p.deadband
            due = now - self.published.get(p.id, -1e10) >= p.publication_s
            if due or changed or severity != previous:
                self.published[p.id] = now
                self.last_values[p.id] = value
                self.outbox[p.id] = m.model_dump(mode="json")
                if self.authoritative:
                    self.ingest(m)

    async def heartbeat(self, force_mesh=False):
        async def exchange(node):
            try:
                # Snapshot deltas are bounded by 500 configured parameters. Live master forwarding uses same data.
                forward = self.master and self.master.get("som_id") == node.id
                full = time.monotonic() - self.last_full_forward > 2
                measurements = (
                    (self.cache.snapshot() if full else list(self.outbox.values())) if forward else []
                )
                result = await self.transport.call(
                    node,
                    "Peer",
                    {"action": "heartbeat", "identity": self.identity(), "measurements": measurements},
                )
                if forward:
                    for item in measurements:
                        current = self.outbox.get(item["parameter"]["id"])
                        if current and current["sequence"] <= item["sequence"]:
                            self.outbox.pop(item["parameter"]["id"], None)
                    if full:
                        self.last_full_forward = time.monotonic()
                identity = result["identity"]
                if identity["lifecycle"] == "QUARANTINED":
                    self.failed_peers.discard(node.id)
                self.peers[node.id], self.seen[node.id] = identity, time.monotonic()
                if identity["role"] == "MASTER":
                    self.master = identity
                return bool(identity.get("eligible"))
            except grpc.RpcError as exc:
                self.peer_errors[node.id] = exc.code().name
                if time.monotonic() - self.seen.get(node.id, 0) > self.peer_failure_s:
                    if self.peers.get(node.id, {}).get("eligible"):
                        self.event("connectivity", f"Peer {node.id} disconnected", "WARNING")
                        self.failed_peers.add(node.id)
                    if node.id in self.peers:
                        self.peers[node.id]["eligible"] = False
                        self.peers[node.id]["connection"] = "DISCONNECTED"

        now = time.monotonic()
        full_mesh = force_mesh or now - self.last_mesh_heartbeat >= self.inventory.peer_heartbeat_s
        if full_mesh:
            self.last_mesh_heartbeat = now
            targets = [n for n in self.members if n.id != self.node.id]
        else:
            master_id = self.master and self.master.get("som_id")
            live_master = self.peers.get(master_id, {})
            targets = (
                [self.by_id[master_id]]
                if (
                    master_id in self.by_id
                    and master_id != self.node.id
                    and live_master.get("role") == "MASTER"
                    and live_master.get("eligible")
                )
                else []
            )
        await asyncio.gather(*(exchange(n) for n in targets))

    async def elect(self):
        if self.had_authority and not self.authoritative:
            # A new tenure must never resurrect commands from an earlier one,
            # even if the same SOM and boot regain leadership later.
            self.generation = str(uuid4())
            self.had_authority = False
        if self.authoritative and self.lease_until - time.monotonic() > self.inventory.lease_s - 1.25:
            return
        if (
            not self.eligible
            or time.monotonic() < self.transfer_hold_until
            and self.preferred_transfer != self.node.id
        ):
            return
        if not self.authoritative and self.preferred() != self.node.id:
            return
        started = time.monotonic()
        authority = {"candidate": self.node.id, "boot_id": self.boot_id, "generation": self.generation}

        async def vote(node):
            try:
                if node.id == self.node.id:
                    result = self.voter.vote(**authority, preferred=self.preferred())
                else:
                    result = await self.transport.call(node, "Peer", {"action": "vote", **authority})
                return node.id if result.get("granted") else None
            except grpc.RpcError:
                return None

        grantors = [x for x in await asyncio.gather(*(vote(n) for n in self.members)) if x]
        if len(grantors) >= len(self.members) // 2 + 1:
            new = not self.authoritative
            self.grantors = grantors
            self.lease_until = started + self.inventory.lease_s - 0.5
            self.had_authority = True
            self.master = self.identity()
            if new:
                self.event("leadership", f"Master elected generation {self.generation}", "INFO")
        # Failed renewal never extends the old bounded lease.

    async def sampling_loop(self):
        while True:
            await self.sample_once()
            await asyncio.sleep(0.02)

    async def control_loop(self):
        while True:
            if self.online and self.lifecycle != Lifecycle.BOOTING:
                self.check_local_recovery()
                await self.heartbeat()
                await self.elect()
            await asyncio.sleep(self.inventory.tick_s)

    async def start(self):
        self.server = grpc.aio.server(
            options=[
                ("grpc.max_receive_message_length", MAX_WIRE + 1024),
                ("grpc.max_send_message_length", MAX_WIRE + 1024),
            ]
        )
        rpc.add_AgentServicer_to_server(self, self.server)
        if self.tls:
            creds = grpc.ssl_server_credentials(
                [(Path(self.tls["key"]).read_bytes(), Path(self.tls["cert"]).read_bytes())],
                root_certificates=Path(self.tls["ca"]).read_bytes(),
                require_client_auth=True,
            )
            port = self.server.add_secure_port(self.node.endpoint, creds)
        else:
            if self.node.endpoint.split(":")[0] not in {"127.0.0.1", "localhost"}:
                raise ValueError("insecure server restricted to loopback")
            port = self.server.add_insecure_port(self.node.endpoint)
        if not port:
            raise RuntimeError("gRPC port unavailable")
        await self.server.start()
        self.tasks = [asyncio.create_task(self.sampling_loop()), asyncio.create_task(self.control_loop())]

    async def close(self):
        for task in [*self.tasks, *self.operation_tasks]:
            task.cancel()
        await asyncio.gather(*self.tasks, *self.operation_tasks, return_exceptions=True)
        if self.server:
            await self.server.stop(0)
        await self.transport.close()
