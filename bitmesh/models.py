"""Versioned wire models; stable IDs are independent of routable addresses."""

from __future__ import annotations

import math
import time
from enum import StrEnum
from ipaddress import IPv4Address
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True, allow_inf_nan=False)


class Lifecycle(StrEnum):
    BOOTING = "BOOTING"
    QUARANTINED = "QUARANTINED"
    READY = "READY"
    ACTIVE = "ACTIVE"
    MAINTENANCE = "MAINTENANCE"
    FAILED = "FAILED"


class Quality(Model):
    state: Literal["SYNCHRONIZED", "DEGRADED", "UNSYNCHRONIZED"] = "SYNCHRONIZED"
    offset_us: float = 0
    uncertainty_us: float = 20
    grandmaster: str | None = "simulated-gm"


class Parameter(Model):
    id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=128)
    description: str = Field(default="", max_length=1024)
    category: str = Field(min_length=1, max_length=64)
    data_type: Literal["boolean", "integer", "float", "string", "enum", "bitfield"]
    unit: str = ""
    collection_s: float = Field(default=0.2, ge=0.02, le=86400)
    publication_s: float = Field(default=0.2, ge=0.02, le=86400)
    retention_s: int = Field(default=86400, ge=0)
    deadband: float = Field(default=0, ge=0)
    warning: float | None = None
    critical: float | None = None

    @model_validator(mode="after")
    def thresholds(self):
        if self.warning is not None and self.critical is not None and self.warning > self.critical:
            raise ValueError("warning must not exceed critical")
        return self


class Measurement(Model):
    som_id: str
    boot_id: str
    parameter: Parameter
    value: bool | int | float | str | None
    source_time: float = Field(default_factory=time.time)
    master_time: float | None = None
    cpu_time: float | None = None
    validity: Literal["VALID", "UNAVAILABLE", "STALE"] = "VALID"
    severity: Literal["INFO", "WARNING", "CRITICAL"] = "INFO"
    quality: Quality = Field(default_factory=Quality)
    sequence: int = 0

    @model_validator(mode="after")
    def typed_value(self):
        if self.value is None:
            if self.validity == "VALID":
                raise ValueError("valid measurement requires a value")
            return self
        kind = self.parameter.data_type
        valid = {
            "boolean": type(self.value) is bool,
            "integer": type(self.value) is int,
            "bitfield": type(self.value) is int and self.value >= 0,
            "float": type(self.value) in (int, float),
            "string": type(self.value) is str,
            "enum": type(self.value) is str,
        }[kind]
        if not valid or isinstance(self.value, float) and not math.isfinite(self.value):
            raise ValueError("BIT value does not match declared type or is non-finite")
        return self


class Event(Model):
    som_id: str
    boot_id: str
    sequence: int = Field(ge=1)
    source_time: float = Field(default_factory=time.time)
    severity: Literal["INFO", "WARNING", "CRITICAL"]
    category: str
    description: str
    parameter_id: str | None = None
    state: Literal["ACTIVE", "CLEARED", "NOTICE"] = "ACTIVE"
    quality: Quality = Field(default_factory=Quality)


class Node(Model):
    id: str = Field(min_length=1, max_length=128)
    subsystem: str = Field(min_length=1, max_length=64)
    ip: IPv4Address
    endpoint: str


class Inventory(Model):
    nodes: list[Node]
    memory_mb: int = Field(default=16, ge=16, le=64)
    lease_s: float = Field(default=3, ge=2, le=5)
    tick_s: float = Field(default=0.2, ge=0.05, le=0.5)
    peer_heartbeat_s: float = Field(default=1.0, ge=0.2, le=2.0)
    rpc_timeout_s: float = Field(default=0.25, ge=0.1, le=0.5)
    parameters: list[Parameter] = Field(default_factory=list)

    @model_validator(mode="after")
    def topology(self):
        if any(node.id == "cpu" for node in self.nodes):
            raise ValueError("SOM identity 'cpu' is reserved for the CPU service certificate")
        if len(self.parameters) > 500 or len({p.id for p in self.parameters}) != len(self.parameters):
            raise ValueError("inventory requires at most 500 uniquely identified parameters")
        if len(self.nodes) > 400 or len({n.subsystem for n in self.nodes}) > 25:
            raise ValueError("inventory exceeds 25 subsystems / 400 SOMs")
        for attr in ("id", "endpoint", "ip"):
            if len({getattr(n, attr) for n in self.nodes}) != len(self.nodes):
                raise ValueError(f"duplicate {attr}")
        for subsystem in {n.subsystem for n in self.nodes}:
            members = [n for n in self.nodes if n.subsystem == subsystem]
            if not 4 <= len(members) <= 16:
                raise ValueError("each subsystem requires 4–16 SOMs")
            ips = sorted(int(n.ip) for n in members)
            if any(b != a + 1 or b // 256 != a // 256 for a, b in zip(ips, ips[1:])):
                raise ValueError("subsystem IPs must be consecutive in the fourth octet")
        return self


class Command(Model):
    id: str = Field(default_factory=lambda: str(uuid4()), min_length=1, max_length=128)
    target: str
    boot_id: str
    kind: Literal[
        "start",
        "stop",
        "restart",
        "reset",
        "configure",
        "update_software",
        "program_pl",
        "approve",
        "transfer",
        "status",
    ]
    operator: str = Field(min_length=1, max_length=128)
    requested_at: float = Field(default_factory=time.time)
    timeout_s: float = Field(default=15, ge=0.1, le=300)
    arguments: dict[str, Any] = Field(default_factory=dict)
    state: Literal[
        "REQUESTED", "VALIDATED", "ACCEPTED", "EXECUTING", "SUCCEEDED", "FAILED", "TIMED_OUT", "UNKNOWN"
    ] = "REQUESTED"
    validation: str | None = None
    error: str | None = None
    completed_at: float | None = None


TRANSITIONS = {
    Lifecycle.BOOTING: {Lifecycle.QUARANTINED, Lifecycle.READY},
    Lifecycle.QUARANTINED: {Lifecycle.READY, Lifecycle.FAILED},
    Lifecycle.READY: {Lifecycle.ACTIVE, Lifecycle.MAINTENANCE, Lifecycle.FAILED, Lifecycle.QUARANTINED},
    Lifecycle.ACTIVE: {Lifecycle.READY, Lifecycle.MAINTENANCE, Lifecycle.FAILED, Lifecycle.QUARANTINED},
    Lifecycle.MAINTENANCE: {Lifecycle.READY, Lifecycle.QUARANTINED, Lifecycle.FAILED},
    Lifecycle.FAILED: {Lifecycle.QUARANTINED},
}


def transition(old: Lifecycle, new: Lifecycle, *, approved: bool = False) -> Lifecycle:
    if (
        new not in TRANSITIONS[old]
        or old == Lifecycle.QUARANTINED
        and new == Lifecycle.READY
        and not approved
    ):
        raise ValueError(f"invalid lifecycle transition {old} -> {new}")
    return new
