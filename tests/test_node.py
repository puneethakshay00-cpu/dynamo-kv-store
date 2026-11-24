"""Targeted scenarios, run on the deterministic simulator."""

import itertools

import pytest

from dynamokv.config import Config
from dynamokv.messages import GET, PUT
from dynamokv.sim import Simulation


class Driver:
    """A scripted client: sends one request and runs the simulation until it settles."""

    def __init__(self, sim: Simulation) -> None:
        self.sim, self.id = sim, "driver"
        self.replies: dict = {}
        self.ids = itertools.count(1)
        sim.clients[self.id] = self

    def handle(self, src, message):
        self.replies[message["req"]] = message

    def call(self, node, message, wait=1.0):
        message["req"] = req = next(self.ids)
        self.sim.send(self.id, node, message)
        self.sim.run(self.sim.time + wait)
        return self.replies.get(req)

    def put(self, key, value, ctx=None, node=None, wait=1.0):
        node = node or self.sim.ring.preference_list(key, self.sim.config.n)[0]
        return self.call(node, {"t": PUT, "key": key, "value": value, "ctx": ctx or {}}, wait)

    def get(self, key, node="n1"):
        return self.call(node, {"t": GET, "key": key})


def values(reply):
    return sorted(v.value for v in reply["versions"] if v.value is not None)


def settle(sim, seconds=1.0):
    sim.run(sim.time + seconds)


QUIET = Config(anti_entropy_interval=1e9)   # isolate one mechanism at a time


def test_put_then_get():
    sim = Simulation(1)
    client = Driver(sim)
    assert client.put("k", b"v1")["ok"]
    reply = client.get("k", node="n5")
    assert reply["ok"] and values(reply) == [b"v1"]
    assert client.put("k", b"v2", reply["ctx"])["ok"]
    assert values(client.get("k")) == [b"v2"]


def test_concurrent_writes_become_siblings_until_merged():
    sim = Simulation(2)
    client = Driver(sim)
    client.put("k", b"base")
    ctx = client.get("k")["ctx"]
    a, b = sim.ring.preference_list("k", 3)[:2]
    client.put("k", b"left", ctx, node=a)
    client.put("k", b"right", ctx, node=b)
    reply = client.get("k")
    assert values(reply) == [b"left", b"right"]
    client.put("k", b"merged", reply["ctx"])
    assert values(client.get("k")) == [b"merged"]


def test_put_to_a_node_that_does_not_hold_the_key_is_redirected():
    sim = Simulation(3)
    client = Driver(sim)
    outsider = next(m for m in sim.members if m not in sim.ring.preference_list("k", 3))
    reply = client.put("k", b"v", node=outsider)
    assert not reply["ok"] and reply["redirect"] == list(sim.ring.preference_list("k", 3))


def test_sloppy_quorum_and_hinted_handoff():
    sim = Simulation(4, QUIET)
    client = Driver(sim)
    coordinator, victim, _ = sim.ring.preference_list("k", 3)
    sim.crash(victim)
    settle(sim)                                     # peers notice it is gone
    assert client.put("k", b"written while down", node=coordinator)["ok"]
    holders = [m for m in sim.members if sim.storage[m].hints.get(victim, {}).get("k")]
    assert holders and victim not in holders
    assert sim.storage[victim].get("k") == []

    sim.restart(victim)
    settle(sim, 2.0)
    assert [v.value for v in sim.storage[victim].get("k")] == [b"written while down"]
    assert sum(s.hint_count() for s in sim.storage.values()) == 0


@pytest.mark.parametrize("w, expected", [(2, True), (3, False)])
def test_strict_quorum_trades_availability_for_consistency(w, expected):
    sim = Simulation(5, Config(w=w, sloppy_quorum=False))
    client = Driver(sim)
    coordinator, victim, _ = sim.ring.preference_list("k", 3)
    sim.crash(victim)
    settle(sim)
    assert client.put("k", b"v", node=coordinator)["ok"] is expected
    assert sum(s.hint_count() for s in sim.storage.values()) == 0    # strict: nobody else holds it


def test_read_repair_fixes_a_stale_replica():
    sim = Simulation(6, QUIET)
    client = Driver(sim)
    client.put("k", b"v")
    stale = sim.ring.preference_list("k", 3)[2]
    sim.storage[stale].data.pop("k")
    assert values(client.get("k")) == [b"v"]
    settle(sim)
    assert [v.value for v in sim.storage[stale].get("k")] == [b"v"]


def test_anti_entropy_repairs_replicas_without_any_reads():
    sim = Simulation(7, Config(read_repair=False))
    client = Driver(sim)
    for i in range(30):
        client.put(f"k{i}", b"v", wait=0.05)
    settle(sim)
    wiped = sim.members[0]
    lost = [k for k in list(sim.storage[wiped].data) if sim.storage[wiped].data.pop(k)]
    assert lost
    settle(sim, 6.0)
    assert all(sim.storage[wiped].get(k) for k in lost)
    assert sum(n.stats["anti_entropy_repairs"] for n in sim.nodes.values()) >= len(lost)


def test_failure_detection_follows_crashes_and_restarts():
    sim = Simulation(8)
    sim.crash("n3")
    settle(sim, 1.5)
    assert not any(node.alive("n3") for node in sim.nodes.values())
    sim.restart("n3")
    settle(sim, 1.5)
    assert all(node.alive("n3") for node in sim.nodes.values())

