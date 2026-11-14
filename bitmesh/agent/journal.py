"""Byte-bounded volatile data. Acknowledgment never implies persistence on SOM."""

from collections import OrderedDict, deque

from bitmesh.models import Event, Measurement


class Journal:
    def __init__(self, budget: int):
        self.budget = budget
        self.events: deque[tuple[Event, int]] = deque()
        self.bytes = 0
        self.sequence = 0
        self.overwritten = 0
        self.gaps: deque[tuple[int, int]] = deque(maxlen=128)
        self.gap_ranges_dropped = 0
        self.acknowledged = 0

    def append(self, **fields) -> Event:
        self.sequence += 1
        event = Event(sequence=self.sequence, **fields)
        size = len(event.model_dump_json().encode()) + 128
        if size > self.budget:
            raise ValueError("event exceeds journal budget")
        while self.bytes + size > self.budget:
            lost, cost = self.events.popleft()
            self.bytes -= cost
            if lost.sequence > self.acknowledged:
                self.overwritten += 1
                if self.gaps and self.gaps[-1][1] + 1 == lost.sequence:
                    self.gaps[-1] = (self.gaps[-1][0], lost.sequence)
                else:
                    if len(self.gaps) == self.gaps.maxlen:
                        self.gap_ranges_dropped += 1
                    self.gaps.append((lost.sequence, lost.sequence))
        self.events.append((event, size))
        self.bytes += size
        return event

    def replay(self, after: int = 0, limit: int = 128) -> dict:
        return {
            "events": [e.model_dump(mode="json") for e, _ in self.events if e.sequence > after][:limit],
            "gaps": list(self.gaps),
            "overwritten": self.overwritten,
            "gap_ranges_dropped": self.gap_ranges_dropped,
            "last_sequence": self.sequence,
        }

    def ack(self, sequence: int):
        if sequence < 0 or sequence > self.sequence:
            raise ValueError("ack outside source sequence range")
        self.acknowledged = max(sequence, self.acknowledged)
        while self.events and self.events[0][0].sequence <= self.acknowledged:
            _, size = self.events.popleft()
            self.bytes -= size


class Cache:
    def __init__(self, budget: int):
        self.budget = budget
        self.current: OrderedDict[str, tuple[Measurement, int]] = OrderedDict()
        self.history: deque[tuple[Measurement, int]] = deque()
        self.bytes = 0
        self.dropped = 0

    def put(self, measurement: Measurement):
        key = measurement.parameter.id
        cost = len(measurement.model_dump_json().encode()) + 128
        old = self.current.pop(key, None)
        if old:
            self.bytes -= old[1]
        while self.bytes + cost > self.budget and self.history:
            self.bytes -= self.history.popleft()[1]
            self.dropped += 1
        while self.bytes + cost > self.budget and self.current:
            _, (_, size) = self.current.popitem(last=False)
            self.bytes -= size
            self.dropped += 1
        if cost <= self.budget:
            self.current[key] = (measurement, cost)
            self.bytes += cost
        if old and self.bytes + old[1] <= self.budget:
            self.history.append(old)
            self.bytes += old[1]

    def snapshot(self) -> list[dict]:
        return [m.model_dump(mode="json") for m, _ in self.current.values()]
