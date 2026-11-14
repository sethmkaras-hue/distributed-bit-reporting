"""Hardware integration boundary. No Kria registers or diagnostics are assumed."""

import math
import time
from typing import Protocol

from bitmesh.models import Parameter, Quality


class Provider(Protocol):
    quality: Quality

    def read(self, parameter: Parameter) -> bool | int | float | str: ...
    async def execute(self, kind: str, arguments: dict) -> dict: ...


def default_parameters(count: int = 12) -> list[Parameter]:
    base = [
        Parameter(
            id="ps.temperature",
            name="PS temperature",
            category="PS",
            data_type="float",
            unit="°C",
            warning=75,
            critical=85,
            deadband=0.1,
        ),
        Parameter(
            id="pl.temperature",
            name="PL temperature",
            category="PL",
            data_type="float",
            unit="°C",
            warning=75,
            critical=85,
            deadband=0.1,
        ),
        Parameter(
            id="cpu.utilization",
            name="CPU utilization",
            category="Resources",
            data_type="float",
            unit="%",
            warning=85,
            critical=95,
        ),
        Parameter(
            id="memory.utilization",
            name="Memory utilization",
            category="Resources",
            data_type="float",
            unit="%",
        ),
        Parameter(
            id="ethernet.errors",
            name="Interface errors",
            category="Interface",
            data_type="integer",
            warning=1,
            critical=10,
        ),
        Parameter(
            id="application.healthy", name="Processing health", category="Application", data_type="boolean"
        ),
        Parameter(
            id="software.version",
            name="Software version",
            category="Versions",
            data_type="string",
            collection_s=5,
            publication_s=5,
        ),
        Parameter(
            id="pl.version",
            name="PL version",
            category="Versions",
            data_type="string",
            collection_s=5,
            publication_s=5,
        ),
    ]
    for i in range(max(0, count - len(base))):
        base.append(
            Parameter(
                id=f"synthetic.{i}",
                name=f"Synthetic diagnostic {i}",
                category="Diagnostics",
                data_type="float",
                unit="units",
                collection_s=1,
                publication_s=1,
            )
        )
    return base


class MockProvider:
    def __init__(self, seed: int = 0):
        self.seed = seed
        self.overrides: dict = {}
        self.quality = Quality()
        self.software = "sim-1.0"
        self.pl = "sim-pl-1.0"

    def read(self, parameter: Parameter):
        if parameter.id in self.overrides:
            return self.overrides[parameter.id]
        wave = math.sin(time.monotonic() / 8 + self.seed)
        fallback = {
            "float": 10 + wave,
            "integer": int(10 + wave),
            "bitfield": 3,
            "boolean": True,
            "string": "simulated",
            "enum": "READY",
        }[parameter.data_type]
        return {
            "ps.temperature": 48 + wave * 3,
            "pl.temperature": 52 + wave * 4,
            "cpu.utilization": 28 + wave * 12,
            "memory.utilization": 42 + wave * 2,
            "ethernet.errors": 0,
            "application.healthy": True,
            "software.version": self.software,
            "pl.version": self.pl,
        }.get(parameter.id, fallback)

    async def execute(self, kind: str, arguments: dict) -> dict:
        if kind in {"program_pl", "update_software"}:
            artifact = arguments.get("artifact")
            if not isinstance(artifact, str) or not artifact.startswith("sim-") or len(artifact) > 128:
                raise ValueError("simulation requires an explicit sim-* artifact identity")
            if kind == "program_pl":
                self.pl = artifact
            else:
                self.software = artifact
            return {"artifact": artifact}
        return {}


class HardwareProvider:
    quality = Quality(state="UNSYNCHRONIZED", uncertainty_us=1e9, grandmaster=None)

    def read(self, parameter):
        raise NotImplementedError("Kria BIT provider not integrated")

    async def execute(self, kind, arguments):
        raise NotImplementedError("Kria control adapter not integrated; operation rejected")
