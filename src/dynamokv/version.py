"""Versions and causality tracking.

Each value a client writes becomes a :class:`Version` tagged with a *dot*: the id of
the node that coordinated the write and a counter. The version also carries the
client's *context*: a version vector summarising every version the client had read.

Two clock policies are implemented:

``DottedVersionVectors`` (default)
    A version supersedes another only if its context covers the other's dot.
    Coordinators draw counters from a durable per-node counter, so two writes that
    did not see each other always stay concurrent (both are kept as siblings).

``PaperVectorClocks``
    The scheme described in the Dynamo paper: the coordinator increments its own
    entry of the client's clock. Two concurrent writes routed through the same
    coordinator from the same context get *identical* clocks, and one of them is
    silently dropped. It is kept so the simulator can demonstrate that failure.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

Context = dict[str, int]


@dataclass(frozen=True)
class Version:
    node: str
    counter: int
    context: Context = field(hash=False, compare=False)
    value: bytes | None  # None marks a delete (tombstone)

    @property
    def dot(self) -> tuple[str, int]:
        return (self.node, self.counter)

    @property
    def clock(self) -> Context:
        """The full vector clock: the context plus this version's own event."""
        clock = dict(self.context)
        if clock.get(self.node, 0) < self.counter:
            clock[self.node] = self.counter
        return clock


class DottedVersionVectors:
    name = "dvv"

    @staticmethod
    def identity(version: Version) -> tuple:
        return version.dot

    @staticmethod
    def supersedes(newer: Version, older: Version) -> bool:
        """True if ``newer`` was written by a client that had seen ``older``."""
        return newer.context.get(older.node, 0) >= older.counter

    @staticmethod
    def next_counter(node: str, context: Context, durable_counter) -> int:
        return durable_counter()


class PaperVectorClocks:
    name = "vclock"

    @staticmethod
    def identity(version: Version) -> tuple:
        return tuple(sorted(version.clock.items()))

    @staticmethod
    def supersedes(newer: Version, older: Version) -> bool:
        a, b = newer.clock, older.clock
        return a != b and all(a.get(node, 0) >= count for node, count in b.items())

    @staticmethod
    def next_counter(node: str, context: Context, durable_counter) -> int:
        return context.get(node, 0) + 1


POLICIES = {policy.name: policy for policy in (DottedVersionVectors, PaperVectorClocks)}


def reconcile(policy, *version_sets: Iterable[Version]) -> list[Version]:
    """Union of the given versions minus every version another one supersedes."""
    by_id: dict[tuple, Version] = {}
    for versions in version_sets:
        for version in versions:
            by_id.setdefault(policy.identity(version), version)
    candidates = list(by_id.values())
    if len(candidates) <= 1:
        return candidates
    return [
        v for v in candidates
        if not any(other is not v and policy.supersedes(other, v) for other in candidates)
    ]


def join_context(versions: Iterable[Version]) -> Context:
    """The context handed back to a client: everything the given versions have seen."""
    joined: Context = {}
    for version in versions:
        for node, count in version.context.items():
            if count > joined.get(node, 0):
                joined[node] = count
        if version.counter > joined.get(version.node, 0):
            joined[version.node] = version.counter
    return joined


def identities(policy, versions: Iterable[Version]) -> frozenset:
    return frozenset(policy.identity(v) for v in versions)
