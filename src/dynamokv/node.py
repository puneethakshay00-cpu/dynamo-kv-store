"""A Dynamo storage node as an event-driven state machine.

The node never blocks and never touches a socket. It reacts to messages and timers
through a small :class:`Runtime` interface (``send``, ``call_later``, ``now``, ``rng``),
so the exact same code runs inside the deterministic simulator and behind the asyncio
TCP server.

Write path (Dynamo paper, 4.4-4.6)
    A put is coordinated by a replica of the key. It creates the new version, merges it
    into its local copy and sends the key's *whole* version set to the other replicas,
    replying once ``W`` copies (itself included) are stored. Replicas that look down are
    replaced by the next healthy nodes on the ring, which keep the data as a *hint* for
    the intended node and hand it back later (sloppy quorum + hinted handoff).

Read path
    Any node can coordinate a get. It asks the first ``N`` healthy nodes, answers once
    ``R`` replied, and afterwards pushes the reconciled versions to stale replicas
    (read repair).

Background
    Gossip spreads heartbeats for failure detection; Merkle-tree anti-entropy repairs
    whatever the other two mechanisms missed.
"""

from __future__ import annotations

import itertools
import random
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Protocol

from . import merkle
from .config import Config
from .messages import (
    AE_DATA,
    AE_DIFF,
    AE_LEAVES,
    AE_SYNC,
    GET,
    GET_REPLY,
    GOSSIP,
    HANDOFF,
    HANDOFF_ACK,
    PUT,
    PUT_REPLY,
    READ,
    READ_REPLY,
    REPAIR,
    REPLICATE,
    REPLICATE_ACK,
    STATS,
    STATS_REPLY,
)
from .ring import Ring
from .version import Version, identities, join_context, reconcile


class Timer(Protocol):
    def cancel(self) -> None: ...


class Runtime(Protocol):
    rng: random.Random

    def now(self) -> float: ...
    def send(self, dst: str, message: dict) -> None: ...
    def call_later(self, delay: float, fn: Callable[[], None]) -> Timer: ...


@dataclass
class _Op:
    """A client request this node is coordinating."""
    id: str
    kind: str
    key: str
    client: str
    req: object
    replied: bool = False
    sent: dict = field(default_factory=dict)      # node asked -> node the data is meant for
    acks: set = field(default_factory=set)        # puts: nodes that stored the write
    replies: dict = field(default_factory=dict)   # gets: node -> versions it returned
    version: Version | None = None
    timers: list = field(default_factory=list)


class Node:
    def __init__(self, node_id: str, members, config: Config, storage, runtime: Runtime) -> None:
        self.id = node_id
        self.cfg = config
        self.ring = Ring(members, config.vnodes)
        if node_id not in self.ring.nodes:
            raise ValueError(f"{node_id} is not a cluster member")
        self.peers = [m for m in self.ring.nodes if m != node_id]
        self.store = storage
        self.policy = storage.policy
        self.rt = runtime
        self.epoch = storage.next_epoch()
        self.stopped = False
        self.stats: Counter = Counter()

        now = runtime.now()
        self.last_seen = dict.fromkeys(self.peers, now)   # assume peers are up until proven otherwise
        self.suspected: dict[str, float] = {}             # peers that missed a deadline we set
        self.beat = 0
        self.beats: dict[str, tuple[int, int]] = {}

        self.ops: dict[str, _Op] = {}
        self._ids = itertools.count(1)
        self.handoffs: dict[str, tuple[str, list]] = {}
        self.handoff_targets: set[str] = set()
        self.ae_out: dict[str, tuple[str, list]] = {}
        self.ae_in: dict[tuple[str, str], list] = {}
        self.handlers = {
            GET: self._on_get, PUT: self._on_put, STATS: self._on_stats,
            REPLICATE: self._on_replicate, REPLICATE_ACK: self._on_replicate_ack,
            READ: self._on_read, READ_REPLY: self._on_read_reply, REPAIR: self._on_repair,
            HANDOFF: self._on_handoff, HANDOFF_ACK: self._on_handoff_ack, GOSSIP: self._on_gossip,
            AE_SYNC: self._on_ae_sync, AE_DIFF: self._on_ae_diff,
            AE_LEAVES: self._on_ae_leaves, AE_DATA: self._on_ae_data,
        }

    # ---------------------------------------------------------------- lifecycle
    def start(self) -> None:
        self._every(self.cfg.gossip_interval, self._gossip)
        self._every(self.cfg.hint_interval, self._handoff)
        self._every(self.cfg.anti_entropy_interval, self._anti_entropy)

    def stop(self) -> None:
        self.stopped = True

    def handle(self, src: str, message: dict) -> None:
        if self.stopped:
            return
        if src in self.last_seen:                 # any message is proof of life
            self.last_seen[src] = self.rt.now()
            self.suspected.pop(src, None)
        self.handlers[message["t"]](src, message)

    def _every(self, interval: float, task: Callable[[], None]) -> None:
        def tick() -> None:
            if self.stopped:
                return
            task()
            self.rt.call_later(interval * (0.75 + 0.5 * self.rt.rng.random()), tick)
        self.rt.call_later(interval * self.rt.rng.random(), tick)

    # ---------------------------------------------------------------- failure detection
    def alive(self, node: str) -> bool:
        if node == self.id:
            return True
        if node in self.suspected:
            return False
        return self.rt.now() - self.last_seen[node] < self.cfg.suspect_after

    def _suspect(self, node: str) -> None:
        if node != self.id and node not in self.suspected:
            self.suspected[node] = self.rt.now()
            self.stats["suspicions"] += 1

    def _gossip(self) -> None:
        self.beat += 1
        table = dict(self.beats)
        table[self.id] = (self.epoch, self.beat)
        self.rt.send(self.rt.rng.choice(self.peers), {"t": GOSSIP, "beats": table})

    def _on_gossip(self, src: str, m: dict) -> None:
        now = self.rt.now()
        for node, beat in m["beats"].items():
            beat = tuple(beat)
            if node != self.id and node in self.last_seen and beat > self.beats.get(node, (0, 0)):
                self.beats[node] = beat
                self.last_seen[node] = now

    # ---------------------------------------------------------------- writes
    def _on_put(self, src: str, m: dict) -> None:
        key = m["key"]
        preference = self.ring.preference_list(key, self.cfg.n)
        if self.id not in preference:
            self.rt.send(src, {"t": PUT_REPLY, "req": m["req"], "ok": False,
                               "error": "not a replica", "redirect": list(preference)})
            return
        self.stats["puts"] += 1
        context = dict(m.get("ctx") or {})
        counter = self.policy.next_counter(self.id, context, self.store.next_counter)
        version = Version(self.id, counter, context, m.get("value"))
        state = self.store.apply(key, [version])

        op = self._op("put", key, src, m["req"])
        op.version = version
        op.acks.add(self.id)
        for node, intended in self._write_targets(key, preference):
            self._replicate(op, node, intended, state)
        self._settle_put(op)
        if op.id in self.ops:
            op.timers.append(self.rt.call_later(self.cfg.fallback_after, lambda: self._put_fallback(op)))
            op.timers.append(self.rt.call_later(self.cfg.request_timeout, lambda: self._put_expire(op)))

    def _write_targets(self, key: str, preference) -> list[tuple[str, str]]:
        """(node to send to, node the copy belongs to) for every replica but this one."""
        spare = [x for x in self.ring.walk(key)[self.cfg.n:] if self.alive(x)] if self.cfg.sloppy_quorum else []
        targets = []
        for node in preference:
            if node == self.id:
                continue
            if self.alive(node):
                targets.append((node, node))
            elif spare:
                targets.append((spare.pop(0), node))
        return targets

    def _eligible(self, key: str) -> tuple[str, ...]:
        """Nodes allowed to hold ``key``: the whole ring walk, or only the N preferred nodes."""
        walk = self.ring.walk(key)
        return walk if self.cfg.sloppy_quorum else walk[: self.cfg.n]

    def _replicate(self, op: _Op, node: str, intended: str, versions: list[Version]) -> None:
        op.sent[node] = intended
        self.rt.send(node, {"t": REPLICATE, "op": op.id, "key": op.key, "versions": versions,
                            "hint": intended if intended != node else None})

    def _put_fallback(self, op: _Op) -> None:
        """Replicas that have not acknowledged in time are replaced by the next healthy nodes."""
        if op.id not in self.ops:
            return
        missing = [(node, intended) for node, intended in op.sent.items() if node not in op.acks]
        if not missing:
            return
        candidates = [x for x in self._eligible(op.key) if x != self.id and x not in op.sent and self.alive(x)]
        state = self.store.get(op.key)
        for node, intended in missing:
            self._suspect(node)
            if candidates:
                self.stats["fallbacks"] += 1
                self._replicate(op, candidates.pop(0), intended, state)

    def _reply_put(self, op: _Op, reply: dict) -> None:
        op.replied = True
        self.rt.send(op.client, reply)

    def _settle_put(self, op: _Op) -> None:
        if not op.replied and len(op.acks) >= self.cfg.w:
            self._reply_put(op, {"t": PUT_REPLY, "req": op.req, "ok": True, "dot": list(op.version.dot)})
        if op.replied and len(op.acks) > len(op.sent):
            self._close(op)

    def _put_expire(self, op: _Op) -> None:
        if op.id in self.ops and not op.replied:
            self.stats["put_timeouts"] += 1
            self._reply_put(op, {"t": PUT_REPLY, "req": op.req, "ok": False,
                                 "error": f"only {len(op.acks)} of {self.cfg.w} replicas acknowledged",
                                 "dot": list(op.version.dot)})
        self._close(op)

    def _on_replicate(self, src: str, m: dict) -> None:
        if m.get("hint") and m["hint"] != self.id:
            self.store.add_hint(m["hint"], m["key"], m["versions"])
        else:
            self.store.apply(m["key"], m["versions"])
        self.rt.send(src, {"t": REPLICATE_ACK, "op": m["op"]})

    def _on_replicate_ack(self, src: str, m: dict) -> None:
        op = self.ops.get(m["op"])
        if op is not None and op.kind == "put":
            op.acks.add(src)
            self._settle_put(op)

    # ---------------------------------------------------------------- reads
    def _on_get(self, src: str, m: dict) -> None:
        self.stats["gets"] += 1
        op = self._op("get", m["key"], src, m["req"])
        for node in [x for x in self._eligible(op.key) if self.alive(x)][: self.cfg.n]:
            self._ask(op, node)
        self._settle_get(op)
        if op.id in self.ops:
            op.timers.append(self.rt.call_later(self.cfg.fallback_after, lambda: self._get_fallback(op)))
            op.timers.append(self.rt.call_later(self.cfg.request_timeout, lambda: self._get_expire(op)))

    def _ask(self, op: _Op, node: str) -> None:
        op.sent[node] = node
        if node == self.id:
            op.replies[node] = self._local_read(op.key)
        else:
            self.rt.send(node, {"t": READ, "op": op.id, "key": op.key})

    def _local_read(self, key: str) -> list[Version]:
        hinted = self.store.hinted(key)
        mine = self.store.get(key)
        return reconcile(self.policy, mine, hinted) if hinted else list(mine)

    def _on_read(self, src: str, m: dict) -> None:
        self.rt.send(src, {"t": READ_REPLY, "op": m["op"], "versions": self._local_read(m["key"])})

    def _on_read_reply(self, src: str, m: dict) -> None:
        op = self.ops.get(m["op"])
        if op is not None and op.kind == "get":
            op.replies[src] = m["versions"]
            self._settle_get(op)

    def _settle_get(self, op: _Op) -> None:
        if not op.replied and len(op.replies) >= self.cfg.r:
            op.replied = True
            versions = reconcile(self.policy, *op.replies.values())
            self.rt.send(op.client, {"t": GET_REPLY, "req": op.req, "ok": True,
                                     "versions": versions, "ctx": join_context(versions)})
        if op.replied and len(op.replies) == len(op.sent):
            self._finish_get(op)

    def _get_fallback(self, op: _Op) -> None:
        if op.id not in self.ops or op.replied:
            return
        silent = [node for node in op.sent if node not in op.replies]
        for node in silent:
            self._suspect(node)
        spare = [x for x in self._eligible(op.key) if x not in op.sent and self.alive(x)]
        for node in spare[: len(silent)]:
            self.stats["fallbacks"] += 1
            self._ask(op, node)
        self._settle_get(op)

    def _get_expire(self, op: _Op) -> None:
        if op.id not in self.ops:
            return
        if not op.replied:
            self.stats["get_timeouts"] += 1
            self.rt.send(op.client, {"t": GET_REPLY, "req": op.req, "ok": False,
                                     "error": f"only {len(op.replies)} of {self.cfg.r} replicas replied"})
        self._finish_get(op)

    def _finish_get(self, op: _Op) -> None:
        """Read repair: push the reconciled versions to every replica that answered stale."""
        self._close(op)
        if not self.cfg.read_repair or not op.replies:
            return
        final = reconcile(self.policy, *op.replies.values())
        final_ids = identities(self.policy, final)
        preference = self.ring.preference_list(op.key, self.cfg.n)
        for node, versions in op.replies.items():
            if node in preference and identities(self.policy, versions) != final_ids:
                self.stats["read_repairs"] += 1
                if node == self.id:
                    self.store.apply(op.key, final)
                else:
                    self.rt.send(node, {"t": REPAIR, "key": op.key, "versions": final})

    def _on_repair(self, src: str, m: dict) -> None:
        self.store.apply(m["key"], m["versions"])

    # ---------------------------------------------------------------- hinted handoff
    def _handoff(self) -> None:
        for target in self.store.hint_targets():
            if target in self.handoff_targets or not self.alive(target):
                continue
            batch = self.store.hints_for(target, self.cfg.handoff_batch)
            batch_id = self._new_id()
            self.handoffs[batch_id] = (target, [(key, identities(self.policy, vs)) for key, vs in batch])
            self.handoff_targets.add(target)
            self.rt.send(target, {"t": HANDOFF, "batch": batch_id, "items": batch})
            self.rt.call_later(self.cfg.request_timeout, lambda b=batch_id: self._handoff_expire(b))

    def _handoff_expire(self, batch_id: str) -> None:
        entry = self.handoffs.pop(batch_id, None)
        if entry is not None:
            self.handoff_targets.discard(entry[0])

    def _on_handoff(self, src: str, m: dict) -> None:
        for key, versions in m["items"]:
            self.store.apply(key, versions)
        self.rt.send(src, {"t": HANDOFF_ACK, "batch": m["batch"]})

    def _on_handoff_ack(self, src: str, m: dict) -> None:
        entry = self.handoffs.pop(m["batch"], None)
        if entry is None:
            return
        target, delivered = entry
        self.handoff_targets.discard(target)
        for key, ids in delivered:
            self.store.remove_hints(target, key, ids)
        self.stats["hints_delivered"] += len(delivered)

    # ---------------------------------------------------------------- anti-entropy
    def _shared(self, peer: str) -> Iterator[tuple[str, list[Version]]]:
        """Keys this node and ``peer`` should both hold."""
        for key, versions in self.store.items():
            preference = self.ring.preference_list(key, self.cfg.n)
            if self.id in preference and peer in preference:
                yield key, versions

    def _leaf_items(self, leaves, peer: str) -> list:
        wanted = set(leaves)
        return [(key, versions) for key, versions in self._shared(peer) if merkle.leaf_of(key) in wanted]

    def _absorb(self, items) -> None:
        for key, versions in items:
            if self.id in self.ring.preference_list(key, self.cfg.n):
                before = identities(self.policy, self.store.get(key))
                if identities(self.policy, self.store.apply(key, versions)) != before:
                    self.stats["anti_entropy_repairs"] += 1

    def _anti_entropy(self) -> None:
        partners = [p for p in self.peers if self.alive(p)]
        if not partners:
            return
        peer = self.rt.rng.choice(partners)
        tree = merkle.build(self._shared(peer), self.policy)
        session = self._new_id()
        self.ae_out[session] = (peer, tree)
        self.rt.send(peer, {"t": AE_SYNC, "sid": session, "level": 0, "idx": [0], "hashes": [tree[0][0]]})
        self.rt.call_later(4 * self.cfg.request_timeout, lambda: self.ae_out.pop(session, None))

    def _on_ae_sync(self, src: str, m: dict) -> None:
        session = (src, m["sid"])
        tree = self.ae_in.get(session)
        if tree is None:
            tree = merkle.build(self._shared(src), self.policy)
            self.ae_in[session] = tree
            self.rt.call_later(4 * self.cfg.request_timeout, lambda: self.ae_in.pop(session, None))
        level = m["level"]
        diff = [i for i, h in zip(m["idx"], m["hashes"], strict=True) if tree[level][i] != h]
        if level == merkle.DEPTH:
            self.rt.send(src, {"t": AE_LEAVES, "sid": m["sid"], "leaves": diff,
                               "items": self._leaf_items(diff, src)})
        else:
            if not diff:
                self.ae_in.pop(session, None)
            self.rt.send(src, {"t": AE_DIFF, "sid": m["sid"], "level": level, "diff": diff})

    def _on_ae_diff(self, src: str, m: dict) -> None:
        entry = self.ae_out.get(m["sid"])
        if entry is None:
            return
        if not m["diff"]:
            self.ae_out.pop(m["sid"], None)
            return
        _, tree = entry
        level = m["level"] + 1
        idx = merkle.children(m["diff"])
        self.rt.send(src, {"t": AE_SYNC, "sid": m["sid"], "level": level, "idx": idx,
                           "hashes": [tree[level][i] for i in idx]})

    def _on_ae_leaves(self, src: str, m: dict) -> None:
        if self.ae_out.pop(m["sid"], None) is None:
            return
        self._absorb(m["items"])
        self.rt.send(src, {"t": AE_DATA, "sid": m["sid"], "items": self._leaf_items(m["leaves"], src)})

    def _on_ae_data(self, src: str, m: dict) -> None:
        self.ae_in.pop((src, m["sid"]), None)
        self._absorb(m["items"])

    # ---------------------------------------------------------------- admin
    def _on_stats(self, src: str, m: dict) -> None:
        self.rt.send(src, {"t": STATS_REPLY, "req": m.get("req"), "node": self.id,
                           "keys": len(self.store.data), "hints": self.store.hint_count(),
                           "up": [p for p in self.peers if self.alive(p)], "stats": dict(self.stats)})

    # ---------------------------------------------------------------- helpers
    def _new_id(self) -> str:
        return f"{self.id}/{self.epoch}/{next(self._ids)}"

    def _op(self, kind: str, key: str, client: str, req) -> _Op:
        op = _Op(self._new_id(), kind, key, client, req)
        self.ops[op.id] = op
        return op

    def _close(self, op: _Op) -> None:
        for timer in op.timers:
            timer.cancel()
        self.ops.pop(op.id, None)
