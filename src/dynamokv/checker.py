"""Checks a finished simulation against the guarantees the store claims.

The checker does not trust the store's own clocks. Every simulated write stores a
unique id and records the ids it had read first, so the true causal history is known
independently. After the cluster is healed and left to settle:

* **no lost writes** - every acknowledged write is still present, or was overwritten
  by a write whose client had (directly or transitively) read it;
* **convergence** - all ``N`` replicas of every key hold identical versions, and no
  hinted replicas are left undelivered;
* **clean siblings** - no surviving version is an ancestor of another survivor;
* **no phantoms** - every surviving value was written by some client.

It also counts *stale reads*: reads that began after a write was acknowledged yet
returned neither that write nor anything derived from it.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field

from .version import identities, reconcile


@dataclass
class Report:
    seed: int
    clock: str
    reads: int = 0
    reads_ok: int = 0
    writes: int = 0
    writes_ok: int = 0
    stale_reads: int = 0
    lost_writes: list = field(default_factory=list)
    unconverged_keys: list = field(default_factory=list)
    unreconciled_keys: list = field(default_factory=list)
    phantom_values: list = field(default_factory=list)
    leftover_hints: int = 0
    max_siblings: int = 0
    faults: list = field(default_factory=list)
    counters: dict = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not (self.lost_writes or self.unconverged_keys or self.unreconciled_keys
                    or self.phantom_values or self.leftover_hints)

    @property
    def availability(self) -> float:
        total = self.reads + self.writes
        return (self.reads_ok + self.writes_ok) / total if total else 1.0

    def summary(self) -> str:
        verdict = "OK " if self.ok else "FAIL"
        return (f"{verdict} seed={self.seed:<6} clock={self.clock:<6} writes={self.writes_ok}/{self.writes} "
                f"reads={self.reads_ok}/{self.reads} lost={len(self.lost_writes)} stale_reads={self.stale_reads} "
                f"unconverged={len(self.unconverged_keys)} hints_left={self.leftover_hints} "
                f"faults={len(self.faults)}")

    def as_dict(self) -> dict:
        data = asdict(self)
        data.update(ok=self.ok, availability=self.availability)
        return data


class _Lineage:
    """Ancestor sets of one key's writes, as bitmasks over the writes' indices."""

    def __init__(self, writes: list[dict]) -> None:
        self.index = {w["wid"]: i for i, w in enumerate(writes)}
        self.parents = {w["wid"]: [p for p in w["parents"] if p in self.index] for w in writes}
        self._memo: dict[str, int] = {}

    def ancestors(self, wid: str) -> int:
        memo = self._memo
        if wid in memo:
            return memo[wid]
        stack = [wid]
        while stack:
            current = stack[-1]
            todo = [p for p in self.parents.get(current, ()) if p not in memo]
            if todo:
                stack.extend(todo)
                continue
            stack.pop()
            mask = 0
            for p in self.parents.get(current, ()):
                mask |= (1 << self.index[p]) | memo[p]
            memo[current] = mask
        return memo[wid]

    def closure(self, wids) -> int:
        """Bitmask of the given writes and everything they descend from."""
        mask = 0
        for wid in wids:
            if wid in self.index:
                mask |= (1 << self.index[wid]) | self.ancestors(wid)
        return mask


def check(sim, history: list[dict], seed: int = 0, faults=()) -> Report:
    report = Report(seed=seed, clock=sim.config.clock, faults=list(faults))
    writes_by_key: dict[str, list] = defaultdict(list)
    reads_by_key: dict[str, list] = defaultdict(list)
    for record in history:
        if record["op"] == "put":
            writes_by_key[record["key"]].append(record)
            report.writes += 1
            report.writes_ok += record["ok"]
        else:
            reads_by_key[record["key"]].append(record)
            report.reads += 1
            report.reads_ok += record["ok"]

    for key in sorted(set(writes_by_key) | set(reads_by_key)):
        writes = writes_by_key.get(key, [])
        lineage = _Lineage(writes)
        replica_sets = [sim.storage[m].get(key) for m in sim.ring.preference_list(key, sim.config.n)]
        if len({identities(sim.policy, s) for s in replica_sets}) > 1:
            report.unconverged_keys.append(key)
        final = reconcile(sim.policy, *replica_sets)
        report.max_siblings = max(report.max_siblings, len(final))
        survivors = [v.value.decode() for v in final if v.value is not None]

        report.phantom_values += [s for s in survivors if s not in lineage.index]
        for s in survivors:
            if any(o != s and lineage.ancestors(o) >> lineage.index.get(s, 0) & 1
                   for o in survivors if o in lineage.index and s in lineage.index):
                report.unreconciled_keys.append(key)
                break

        kept = lineage.closure(survivors)
        report.lost_writes += [w["wid"] for w in writes if w["ok"] and not kept >> lineage.index[w["wid"]] & 1]

        acknowledged = sorted((w["end"], lineage.index[w["wid"]]) for w in writes if w["ok"])
        for read in reads_by_key.get(key, []):
            if not read["ok"]:
                continue
            seen = lineage.closure(read["values"])
            for end, i in acknowledged:
                if end >= read["start"]:
                    break
                if not seen >> i & 1:
                    report.stale_reads += 1
                    break

    report.leftover_hints = sum(store.hint_count() for store in sim.storage.values())
    counters: Counter = Counter(sim.stats)
    for node in sim.nodes.values():
        counters.update(node.stats)
    report.counters = dict(counters)
    return report
