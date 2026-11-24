"""Runs the simulation experiments behind the README and writes results/simulation.json.

    python -m dynamokv.report --seeds 10000 --workers 4
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections import Counter
from pathlib import Path

from .config import Config
from .experiments import sweep

EXPERIMENTS = {
    # name: (config, faults, share of the headline seed count)
    "dvv_faults": (Config(), True, 1.0),
    "dvv_strict_faults": (Config(sloppy_quorum=False), True, 0.1),
    "dvv_no_faults": (Config(), False, 0.1),
    "vclock_no_faults": (Config(clock="vclock"), False, 0.1),
    "vclock_faults": (Config(clock="vclock"), True, 0.1),
}


def summarise(reports, config: Config, faults: bool, seconds: float) -> dict:
    counters: Counter = Counter()
    for report in reports:
        counters.update(report.counters)
    writes = sum(r.writes for r in reports)
    writes_ok = sum(r.writes_ok for r in reports)
    reads = sum(r.reads for r in reports)
    reads_ok = sum(r.reads_ok for r in reports)
    return {
        "clock": config.clock, "sloppy_quorum": config.sloppy_quorum, "faults": faults,
        "n": config.n, "r": config.r, "w": config.w,
        "scenarios": len(reports),
        "scenarios_failed": sum(not r.ok for r in reports),
        "scenarios_with_lost_writes": sum(bool(r.lost_writes) for r in reports),
        "writes": writes, "writes_acknowledged": writes_ok,
        "lost_writes": sum(len(r.lost_writes) for r in reports),
        "reads": reads, "reads_ok": reads_ok,
        "stale_reads": sum(r.stale_reads for r in reports),
        "availability": round((writes_ok + reads_ok) / max(1, writes + reads), 5),
        "write_availability": round(writes_ok / max(1, writes), 5),
        "unconverged_keys": sum(len(r.unconverged_keys) for r in reports),
        "leftover_hints": sum(r.leftover_hints for r in reports),
        "max_siblings": max((r.max_siblings for r in reports), default=0),
        "faults_injected": sum(len(r.faults) for r in reports),
        "crashes": counters["crashes"], "partitions": counters["partitions"],
        "messages": counters["messages"], "messages_dropped": counters["dropped"],
        "hints_delivered": counters["hints_delivered"], "read_repairs": counters["read_repairs"],
        "anti_entropy_repairs": counters["anti_entropy_repairs"], "fallbacks": counters["fallbacks"],
        "seconds": round(seconds, 1),
        "failed_seeds": [r.seed for r in reports if not r.ok][:50],
    }


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seeds", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 1)
    parser.add_argument("--out", default="results/simulation.json")
    args = parser.parse_args(argv)

    results = {}
    for name, (config, faults, share) in EXPERIMENTS.items():
        started = time.time()
        reports = sweep(range(max(1, int(args.seeds * share))), config, faults=faults, workers=args.workers)
        results[name] = summarise(reports, config, faults, time.time() - started)
        r = results[name]
        print(f"{name:18} {r['scenarios']:>6} scenarios  acked={r['writes_acknowledged']:>9,}  "
              f"lost={r['lost_writes']:>6,}  stale={r['stale_reads']:>7,}/{r['reads_ok']:,}  "
              f"avail={r['availability']:.2%}  failed={r['scenarios_failed']}  {r['seconds']}s", flush=True)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(results, indent=2))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
