"""Fixed-membership, boot-fenced monotonic leases. No external consensus service."""

import time
from dataclasses import dataclass


@dataclass
class Grant:
    candidate: str
    boot_id: str
    generation: str
    until: float


class Voter:
    def __init__(self, lease_s: float = 3, now=time.monotonic):
        self.now = now
        self.lease_s = lease_s
        self.blackout_until = now() + lease_s
        self.grant: Grant | None = None

    def vote(self, candidate: str, boot_id: str, generation: str, preferred: str | None) -> dict:
        now = self.now()
        if now < self.blackout_until:
            return {"granted": False}
        active = self.grant and self.grant.until > now
        same = active and (self.grant.candidate, self.grant.boot_id, self.grant.generation) == (
            candidate,
            boot_id,
            generation,
        )
        if active and not same or not active and candidate != preferred:
            return {"granted": False}
        self.grant = Grant(candidate, boot_id, generation, now + self.lease_s)
        return {"granted": True, "valid_for_s": self.lease_s}

    def verify(self, candidate: str, boot_id: str, generation: str) -> bool:
        g = self.grant
        return bool(
            g
            and self.now() < g.until
            and (g.candidate, g.boot_id, g.generation) == (candidate, boot_id, generation)
        )
