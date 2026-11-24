"""Deterministic fault-injection simulator.

Every node runs the real :class:`~dynamokv.node.Node` code, but messages and timers go
through a single event queue in virtual time, driven by one seeded random generator.
The simulator can crash and restart nodes (their storage survives, like a disk),
partition the network, drop, duplicate and delay messages. A given seed always
produces exactly the same run, so any failure the checker finds can be replayed.
"""

from __future__ import annotations

import heapq
import itertools
import random
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

from .config import Config
from .messages import GET, GET_REPLY, PUT, PUT_REPLY
from .node import Node
from .ring import Ring
from .storage import MemoryStorage
from .version import POLICIES


class _Timer:
    __slots__ = ("cancelled",)

    def __init__(self) -> None:
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


class _NodeRuntime:
    """What a node sees of the world inside the simulator."""

    def __init__(self, sim: Simulation, node_id: str, incarnation: int) -> None:
        self.sim, self.node_id, self.incarnation = sim, node_id, incarnation
        self.rng = random.Random(f"{sim.seed}/{node_id}/{incarnation}")

    def now(self) -> float:
        return self.sim.time

    def send(self, dst: str, message: dict) -> None:
        self.sim.send(self.node_id, dst, message)

    def call_later(self, delay: float, fn: Callable[[], None]) -> _Timer:
        def fire() -> None:  # timers die with the incarnation that set them
            if self.sim.incarnation[self.node_id] == self.incarnation and self.node_id in self.sim.nodes:
                fn()
        return self.sim.at(delay, fire)


class Simulation:
    def __init__(self, seed: int, config: Config = Config(), nodes: int = 5) -> None:
        self.seed = seed
        self.config = config
        self.rng = random.Random(seed)
        self.time = 0.0
        self._queue: list = []
        self._seq = itertools.count()
        self.members = [f"n{i}" for i in range(1, nodes + 1)]
        self.ring = Ring(self.members, config.vnodes)
        self.policy = POLICIES[config.clock]
        self.storage = {m: MemoryStorage(self.policy) for m in self.members}
        self.nodes: dict[str, Node] = {}
        self.incarnation: Counter = Counter()
        self.clients: dict[str, object] = {}
        self.groups: list[frozenset] | None = None     # network partition, if any
        self.drop_rate = 0.0
        self.duplicate_rate = 0.0
        self.slow_rate = 0.01
        self.stats: Counter = Counter()
        for member in self.members:
            self.start_node(member)

    # ---------------------------------------------------------------- event loop
    def at(self, delay: float, fn: Callable[[], None]) -> _Timer:
        timer = _Timer()
        heapq.heappush(self._queue, (self.time + delay, next(self._seq), timer, fn))
        return timer

    def run(self, until: float) -> None:
        queue = self._queue
        while queue and queue[0][0] <= until:
            when, _, timer, fn = heapq.heappop(queue)
            self.time = when
            if not timer.cancelled:
                fn()
        self.time = until

    # ---------------------------------------------------------------- network
    def _latency(self) -> float:
        delay = 0.0005 + self.rng.expovariate(1 / 0.002)
        if self.rng.random() < self.slow_rate:
            delay += self.rng.uniform(0.02, 0.15)
        return delay

    def _connected(self, a: str, b: str) -> bool:
        if self.groups is None or a in self.clients or b in self.clients:
            return True
        return any(a in group and b in group for group in self.groups)

    def send(self, src: str, dst: str, message: dict) -> None:
        self.stats["messages"] += 1
        if not self._connected(src, dst) or self.rng.random() < self.drop_rate:
            self.stats["dropped"] += 1
            return
        copies = 2 if self.rng.random() < self.duplicate_rate else 1
        for _ in range(copies):
            self.at(self._latency(), lambda: self._deliver(src, dst, message))

    def _deliver(self, src: str, dst: str, message: dict) -> None:
        if not self._connected(src, dst):
            self.stats["dropped"] += 1
            return
        target = self.nodes.get(dst) or self.clients.get(dst)
        if target is not None:
            target.handle(src, message)

    # ---------------------------------------------------------------- faults
    def start_node(self, member: str) -> None:
        self.incarnation[member] += 1
        node = Node(member, self.members, self.config, self.storage[member],
                    _NodeRuntime(self, member, self.incarnation[member]))
        self.nodes[member] = node
        node.start()

    def crash(self, member: str) -> None:
        node = self.nodes.pop(member, None)
        if node is not None:
            node.stop()
            self.incarnation[member] += 1
            self.stats["crashes"] += 1

    def restart(self, member: str) -> None:
        if member not in self.nodes:
            self.start_node(member)
            self.stats["restarts"] += 1

    def partition(self, *groups) -> None:
        self.groups = [frozenset(g) for g in groups]
        self.stats["partitions"] += 1

    def heal(self) -> None:
        self.groups = None
        self.drop_rate = 0.0
        self.duplicate_rate = 0.0
        for member in self.members:
            self.restart(member)

    def down(self) -> list[str]:
        return [m for m in self.members if m not in self.nodes]


@dataclass
class WorkloadSettings:
    clients: int = 6
    keys: int = 20
    duration: float = 6.0          # seconds of client traffic (virtual time)
    settle: float = 10.0           # quiet time after healing, for repair to finish
    think: float = 0.005           # client pause between operations
    read_only: float = 0.3         # share of operations that only read
    client_timeout: float = 0.6
    avoid_for: float = 2.0         # skip a node for this long after it timed out on us


class Client:
    """A simulated client doing read-modify-write cycles: get a key, then put a new value
    with the context the get returned. Every value is a unique write id, and the client
    records which write ids it had read, which gives the checker the true causal history.
    """

    def __init__(self, sim: Simulation, client_id: str, settings: WorkloadSettings, history: list) -> None:
        self.sim, self.id, self.cfg, self.history = sim, client_id, settings, history
        self.rng = random.Random(f"{sim.seed}/{client_id}")
        self.keys = [f"key-{i}" for i in range(settings.keys)]
        self.pending: dict[int, tuple[dict, Callable[[dict, dict], None]]] = {}
        self.ids = itertools.count(1)
        self.writes = itertools.count(1)
        self.avoid: dict[str, float] = {}   # node -> time until which we skip it
        sim.clients[client_id] = self

    def _pick(self, candidates) -> str:
        """Like a partition-aware client library: skip nodes that recently timed out."""
        usable = [n for n in candidates if self.avoid.get(n, -1.0) <= self.sim.time]
        return self.rng.choice(usable or list(candidates))

    def start(self) -> None:
        self.sim.at(self.rng.random() * 0.05, self._next)

    def _next(self) -> None:
        if self.sim.time >= self.cfg.duration:
            return
        key = self.rng.choice(self.keys)
        then = self._after_read if self.rng.random() >= self.cfg.read_only else self._pause
        self._request({"t": GET, "key": key}, {"op": "get", "key": key}, self._pick(self.sim.members), then)

    def _request(self, message: dict, record: dict, node: str, then) -> None:
        req = next(self.ids)
        message["req"] = req
        record.update(start=self.sim.time, node=node, ok=False)
        self.pending[req] = (record, then)
        self.sim.send(self.id, node, message)
        self.sim.at(self.cfg.client_timeout, lambda: self._timeout(req))

    def _timeout(self, req: int) -> None:
        entry = self.pending.pop(req, None)
        if entry is not None:
            record, then = entry
            self.avoid[record["node"]] = self.sim.time + self.cfg.avoid_for
            record.update(end=self.sim.time, error="client timeout")
            self.history.append(record)
            then(record, {})

    def handle(self, src: str, message: dict) -> None:
        entry = self.pending.pop(message.get("req"), None)
        if entry is None:
            return  # a late or duplicated reply
        record, then = entry
        record.update(end=self.sim.time, ok=message["ok"])
        if message["t"] == GET_REPLY and message["ok"]:
            record["values"] = sorted(v.value.decode() for v in message["versions"] if v.value is not None)
        elif message["t"] == PUT_REPLY and "dot" in message:
            record["dot"] = tuple(message["dot"])
        if not message["ok"]:
            record["error"] = message.get("error", "")
        self.history.append(record)
        then(record, message)

    def _pause(self, record: dict, message: dict) -> None:
        self.sim.at(self.cfg.think * (0.5 + self.rng.random()), self._next)

    def _after_read(self, read: dict, message: dict) -> None:
        if not read["ok"]:
            self._pause(read, message)
            return
        wid = f"{self.id}.{next(self.writes)}"
        replicas = self.sim.ring.preference_list(read["key"], self.sim.config.n)
        put = {"t": PUT, "key": read["key"], "value": wid.encode(), "ctx": message["ctx"]}
        record = {"op": "put", "key": read["key"], "wid": wid, "parents": read["values"]}
        self._request(put, record, self._pick(replicas), self._pause)


def random_faults(sim: Simulation, rng: random.Random, start: float, end: float, max_down: int = 2) -> list:
    """Schedule a random mix of crashes, restarts, partitions and lossy periods."""
    plan = []
    t = start
    while True:
        t += rng.uniform(0.2, 1.0)
        if t >= end:
            break
        kind = rng.choice(["crash", "crash", "restart", "partition", "heal", "lossy", "calm"])
        plan.append((round(t, 3), kind))

        def apply(kind=kind) -> None:
            down = sim.down()
            if kind == "crash" and len(down) < max_down:
                sim.crash(rng.choice([m for m in sim.members if m not in down]))
            elif kind == "restart" and down:
                sim.restart(rng.choice(down))
            elif kind == "partition":
                members = list(sim.members)
                rng.shuffle(members)
                cut = rng.randint(1, len(members) - 1)
                sim.partition(members[:cut], members[cut:])
            elif kind == "heal":
                sim.groups = None
            elif kind == "lossy":
                sim.drop_rate, sim.duplicate_rate = rng.uniform(0.05, 0.3), rng.uniform(0.0, 0.1)
            elif kind == "calm":
                sim.drop_rate = sim.duplicate_rate = 0.0
        sim.at(t - sim.time, apply)
    return plan


def run_scenario(seed: int, config: Config = Config(), settings: WorkloadSettings = WorkloadSettings(),
                 faults: bool = True, nodes: int = 5):
    """Run one seeded scenario and return the checker's report."""
    from .checker import check

    sim = Simulation(seed, config, nodes)
    history: list = []
    for i in range(settings.clients):
        Client(sim, f"c{i + 1}", settings, history).start()
    plan = random_faults(sim, random.Random(f"faults/{seed}"), 0.3, settings.duration) if faults else []
    sim.run(settings.duration)
    sim.heal()
    sim.run(settings.duration + settings.settle)
    return check(sim, history, seed=seed, faults=plan)
