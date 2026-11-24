"""Merkle trees for anti-entropy (Dynamo paper, section 4.7).

Two replicas build a tree over the keys they share: each of the 2**DEPTH leaves
hashes the keys (and version ids) that fall in its bucket, and every inner node
hashes its two children. Replicas compare roots first and only descend into
subtrees that differ, so in-sync replicas exchange a single hash.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable

from .ring import hash64
from .version import Version

DEPTH = 6
LEAVES = 1 << DEPTH


def leaf_of(key: str) -> int:
    return hash64("merkle:" + key) >> (64 - DEPTH)


def build(entries: Iterable[tuple[str, list[Version]]], policy) -> list[list[bytes]]:
    """Return the tree as levels: ``tree[0] == [root]``, ``tree[DEPTH]`` are the leaves."""
    buckets: list[list] = [[] for _ in range(LEAVES)]
    for key, versions in entries:
        ids = sorted(repr(policy.identity(v)) for v in versions)
        buckets[leaf_of(key)].append([key, ids])
    level = [hashlib.sha1(json.dumps(sorted(b)).encode()).digest() for b in buckets]
    tree = [level]
    while len(level) > 1:
        level = [hashlib.sha1(level[i] + level[i + 1]).digest() for i in range(0, len(level), 2)]
        tree.insert(0, level)
    return tree


def children(indices: Iterable[int]) -> list[int]:
    return [child for i in indices for child in (2 * i, 2 * i + 1)]
