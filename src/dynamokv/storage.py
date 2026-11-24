"""Per-node storage: the key's versions, hinted replicas held for other nodes, and the
node's durable write counter.

``MemoryStorage`` keeps everything in dictionaries; in the simulator it survives a
simulated crash, which models a disk. ``FileStorage`` adds an append-only write-ahead
log that is replayed on start-up, so a killed node process comes back with its data.
"""

from __future__ import annotations

import base64
import json
import os
from collections.abc import Iterable, Iterator
from pathlib import Path

from .version import Version, identities, reconcile


def encode_version(v: Version) -> list:
    value = None if v.value is None else base64.b64encode(v.value).decode("ascii")
    return [v.node, v.counter, v.context, value]


def decode_version(data: list) -> Version:
    node, counter, context, value = data
    return Version(node, int(counter), {k: int(c) for k, c in context.items()},
                   None if value is None else base64.b64decode(value))


class MemoryStorage:
    def __init__(self, policy) -> None:
        self.policy = policy
        self.data: dict[str, list[Version]] = {}
        self.hints: dict[str, dict[str, list[Version]]] = {}
        self.counter = 0
        self.epoch = 0

    # -- the node's own replicas -------------------------------------------------
    def get(self, key: str) -> list[Version]:
        return self.data.get(key, [])

    def items(self) -> Iterator[tuple[str, list[Version]]]:
        return iter(list(self.data.items()))

    def apply(self, key: str, versions: Iterable[Version]) -> list[Version]:
        """Merge ``versions`` into the key; returns the key's new version set."""
        current = self.data.get(key, [])
        merged = reconcile(self.policy, current, versions)
        if identities(self.policy, merged) != identities(self.policy, current):
            self.data[key] = merged
            self._log_key(key, merged)
        return merged

    def next_counter(self) -> int:
        self.counter += 1
        self._log({"c": self.counter})
        return self.counter

    def next_epoch(self) -> int:
        """Incarnation number, bumped every time the node starts."""
        self.epoch += 1
        self._log({"e": self.epoch})
        return self.epoch

    # -- hinted handoff ------------------------------------------------------------
    def add_hint(self, target: str, key: str, versions: Iterable[Version]) -> None:
        held = self.hints.setdefault(target, {})
        current = held.get(key, [])
        merged = reconcile(self.policy, current, versions)
        if identities(self.policy, merged) != identities(self.policy, current):
            held[key] = merged
            self._log_hint(target, key, merged)

    def hinted(self, key: str) -> list[Version]:
        """Every hinted version of ``key`` this node holds, whoever it is for."""
        found: list[Version] = []
        for held in self.hints.values():
            found.extend(held.get(key, ()))
        return found

    def hint_targets(self) -> list[str]:
        return [target for target, held in self.hints.items() if held]

    def hints_for(self, target: str, limit: int) -> list[tuple[str, list[Version]]]:
        held = self.hints.get(target, {})
        return [(key, list(versions)) for key, versions in list(held.items())[:limit]]

    def remove_hints(self, target: str, key: str, delivered: frozenset) -> None:
        held = self.hints.get(target, {})
        if key not in held:
            return
        remaining = [v for v in held[key] if self.policy.identity(v) not in delivered]
        if remaining:
            held[key] = remaining
        else:
            del held[key]
        self._log_hint(target, key, remaining)

    def hint_count(self) -> int:
        return sum(len(held) for held in self.hints.values())

    # -- persistence hooks (no-ops in memory) -------------------------------------
    def _log_key(self, key: str, versions: list[Version]) -> None:
        self._log({"k": key, "v": [encode_version(v) for v in versions]})

    def _log_hint(self, target: str, key: str, versions: list[Version]) -> None:
        self._log({"h": target, "k": key, "v": [encode_version(v) for v in versions]})

    def _log(self, record: dict) -> None:
        pass


class FileStorage(MemoryStorage):
    """MemoryStorage backed by an append-only JSON-lines write-ahead log."""

    def __init__(self, policy, directory: str | os.PathLike, fsync: bool = False) -> None:
        super().__init__(policy)
        self.path = Path(directory) / "wal.jsonl"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.fsync = fsync
        self._replay()
        self._file = open(self.path, "a", encoding="utf-8")  # noqa: SIM115 - kept open for the node's life

    def _replay(self) -> None:
        if not self.path.exists():
            return
        with open(self.path, encoding="utf-8") as wal:
            for line in wal:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    break  # torn final write after a crash: everything before it is intact
                if "c" in record:
                    self.counter = max(self.counter, record["c"])
                elif "e" in record:
                    self.epoch = max(self.epoch, record["e"])
                elif "h" in record:
                    held = self.hints.setdefault(record["h"], {})
                    versions = [decode_version(v) for v in record["v"]]
                    if versions:
                        held[record["k"]] = versions
                    else:
                        held.pop(record["k"], None)
                else:
                    self.data[record["k"]] = [decode_version(v) for v in record["v"]]

    def _log(self, record: dict) -> None:
        self._file.write(json.dumps(record, separators=(",", ":")) + "\n")
        self._file.flush()
        if self.fsync:
            os.fsync(self._file.fileno())

    def close(self) -> None:
        self._file.close()
