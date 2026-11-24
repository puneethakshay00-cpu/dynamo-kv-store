"""Consistent hashing with virtual nodes (Dynamo paper, section 4.2).

Every physical node owns ``vnodes`` tokens on a 64-bit ring. A key is stored on the
first ``N`` distinct physical nodes found walking clockwise from the key's hash:
its *preference list*. Nodes further along the walk are the fallbacks used by the
sloppy quorum when a preferred node is down.
"""

from __future__ import annotations

import bisect
import hashlib
from collections.abc import Iterable


def hash64(data: str) -> int:
    """Stable 64-bit position on the ring (first 8 bytes of MD5, as in Dynamo)."""
    return int.from_bytes(hashlib.md5(data.encode("utf-8")).digest()[:8], "big")


class Ring:
    def __init__(self, nodes: Iterable[str], vnodes: int = 32) -> None:
        self.nodes: tuple[str, ...] = tuple(sorted(set(nodes)))
        if not self.nodes:
            raise ValueError("a ring needs at least one node")
        if vnodes < 1:
            raise ValueError("vnodes must be positive")
        self.vnodes = vnodes
        tokens = sorted((hash64(f"{node}#{i}"), node) for node in self.nodes for i in range(vnodes))
        self._positions = [position for position, _ in tokens]
        self._owners = [node for _, node in tokens]
        self._walks: dict[str, tuple[str, ...]] = {}

    def walk(self, key: str) -> tuple[str, ...]:
        """All physical nodes in clockwise order from the key: preferred first, then fallbacks."""
        cached = self._walks.get(key)
        if cached is not None:
            return cached
        start = bisect.bisect_right(self._positions, hash64(key))
        seen: list[str] = []
        total = len(self._owners)
        for step in range(total):
            node = self._owners[(start + step) % total]
            if node not in seen:
                seen.append(node)
                if len(seen) == len(self.nodes):
                    break
        order = tuple(seen)
        if len(self._walks) < 200_000:
            self._walks[key] = order
        return order

    def preference_list(self, key: str, n: int) -> tuple[str, ...]:
        """The ``n`` nodes responsible for ``key``."""
        return self.walk(key)[:n]

    def ownership(self) -> dict[str, float]:
        """Share of the hash space each node is the first replica for."""
        share = dict.fromkeys(self.nodes, 0.0)
        span = 1 << 64
        for i, position in enumerate(self._positions):
            previous = self._positions[i - 1] if i else self._positions[-1] - span
            share[self._owners[i]] += (position - previous) / span
        return share
