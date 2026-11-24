"""Benchmarks against a real multi-process cluster on this machine.

1. Throughput and latency for N=3 with (R, W) = (1, 1), (2, 2) and (3, 3).
2. Availability: a 5-node cluster under load; one node is killed with SIGKILL and later
   restarted. Records per-second success rate, latency and outstanding hinted replicas.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import platform
import random
import shutil
import socket
import statistics
import subprocess
import time
from pathlib import Path

from .client import Client, Unavailable
from .config import Config

QUORUMS = [(1, 1), (2, 2), (3, 3)]


def _base_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return 20000 + s.getsockname()[1] % 20000


def _percentile(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


class LocalCluster:
    def __init__(self, directory: Path, config: Config, nodes: int = 5) -> None:
        from .cli import load_cluster, write_cluster_file

        shutil.rmtree(directory, ignore_errors=True)
        self.directory = directory
        self.config = config
        self.file = write_cluster_file(directory, nodes, _base_port(), config)
        self.addresses, _ = load_cluster(self.file)
        self.procs: dict[str, subprocess.Popen] = {}

    def start(self, node: str | None = None) -> None:
        from .cli import start_node_process

        for name in [node] if node else list(self.addresses):
            self.procs[name] = start_node_process(self.file, name, self.directory)

    def kill(self, node: str) -> None:
        self.procs[node].kill()
        self.procs[node].wait()

    def stop(self) -> None:
        for proc in self.procs.values():
            proc.terminate()
        for proc in self.procs.values():
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    async def wait_ready(self, timeout: float = 30.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        async with Client(self.addresses, self.config, timeout=1.0) as client:
            while True:
                try:
                    for node in self.addresses:
                        await client.stats(node)
                    return
                except (OSError, asyncio.TimeoutError) as error:
                    if loop.time() > deadline:
                        raise RuntimeError(f"cluster did not come up: {error!r}") from error
                    await asyncio.sleep(0.2)


async def _load(cluster: LocalCluster, duration: float, concurrency: int, keys: int, value_size: int,
                events: list | None = None) -> list[tuple]:
    """Each worker loops: get a random key, then put a new value with the returned context."""
    records: list[tuple] = []          # (finished at, kind, latency, ok)
    loop = asyncio.get_running_loop()
    start = loop.time()
    value = os.urandom(value_size // 2).hex().encode()

    async def worker(seed: int) -> None:
        rng = random.Random(seed)
        async with Client(cluster.addresses, cluster.config, timeout=1.0) as client:
            while loop.time() - start < duration:
                key = f"user:{rng.randrange(keys)}"
                for kind in ("get", "put"):
                    t0 = loop.time()
                    try:
                        if kind == "get":
                            context = (await client.get(key)).context
                        else:
                            await client.put(key, value, context)
                        ok = True
                    except (Unavailable, OSError, asyncio.TimeoutError):
                        ok = False
                    records.append((loop.time() - start, kind, loop.time() - t0, ok))
                    if not ok:
                        break

    async def scheduler() -> None:
        for at, action in sorted(events or [], key=lambda e: e[0]):
            await asyncio.sleep(max(0.0, at - (loop.time() - start)))
            action()

    await asyncio.gather(scheduler(), *(worker(i) for i in range(concurrency)))
    return records


def _summary(records: list[tuple], duration: float) -> dict:
    out = {"ops_per_sec": round(sum(r[3] for r in records) / duration),
           "errors": sum(not r[3] for r in records)}
    for kind in ("get", "put"):
        latencies = [r[2] * 1000 for r in records if r[1] == kind and r[3]]
        out[kind] = {"count": len(latencies),
                     "p50_ms": round(_percentile(latencies, 0.50), 2),
                     "p95_ms": round(_percentile(latencies, 0.95), 2),
                     "p99_ms": round(_percentile(latencies, 0.99), 2),
                     "mean_ms": round(statistics.fmean(latencies), 2) if latencies else None}
    return out


async def quorum_benchmark(work: Path, duration: float, concurrency: int, keys: int, value_size: int) -> list[dict]:
    results = []
    for r, w in QUORUMS:
        config = Config(n=3, r=r, w=w)
        cluster = LocalCluster(work / f"quorum-r{r}w{w}", config)
        cluster.start()
        try:
            await cluster.wait_ready()
            await _load(cluster, 2.0, concurrency, keys, value_size)          # warm-up
            records = await _load(cluster, duration, concurrency, keys, value_size)
            summary = {"n": 3, "r": r, "w": w, **_summary(records, duration)}
            results.append(summary)
            print(f"N=3 R={r} W={w}: {summary['ops_per_sec']:,} ops/s, get p50/p99 "
                  f"{summary['get']['p50_ms']}/{summary['get']['p99_ms']} ms, put p50/p99 "
                  f"{summary['put']['p50_ms']}/{summary['put']['p99_ms']} ms, errors {summary['errors']}")
        finally:
            cluster.stop()
    return results


async def failure_benchmark(work: Path, duration: float, concurrency: int, keys: int, value_size: int) -> dict:
    config = Config()
    cluster = LocalCluster(work / "failure", config)
    cluster.start()
    victim = "n3"
    kill_at, restart_at = duration * 0.3, duration * 0.6
    hint_samples: list[tuple[float, int]] = []
    try:
        await cluster.wait_ready()
        await _load(cluster, 2.0, concurrency, keys, value_size)
        loop = asyncio.get_running_loop()
        start = loop.time()

        async def sample_hints() -> None:
            async with Client(cluster.addresses, config, timeout=0.3) as client:
                while loop.time() - start < duration + 5:
                    total = 0
                    for node in cluster.addresses:
                        with contextlib.suppress(OSError, asyncio.TimeoutError):
                            total += (await client.stats(node))["hints"]
                    hint_samples.append((round(loop.time() - start, 2), total))
                    await asyncio.sleep(0.25)

        sampler = asyncio.create_task(sample_hints())
        records = await _load(cluster, duration, concurrency, keys, value_size,
                              events=[(kill_at, lambda: cluster.kill(victim)),
                                      (restart_at, lambda: cluster.start(victim))])
        await sampler
    finally:
        cluster.stop()

    seconds = []
    for second in range(int(duration)):
        bucket = [r for r in records if second <= r[0] < second + 1]
        ok = [r for r in bucket if r[3]]
        seconds.append({"t": second, "ops": len(ok), "errors": len(bucket) - len(ok),
                        "success_rate": round(len(ok) / len(bucket), 5) if bucket else None,
                        "p99_ms": round(_percentile([r[2] * 1000 for r in ok], 0.99), 2)})
    drained = next((t for t, h in hint_samples if t > restart_at and h == 0), None)
    down = [r for r in records if kill_at <= r[0] < restart_at]
    summary = {"victim": victim, "kill_at": kill_at, "restart_at": restart_at, "duration": duration,
               "success_rate_overall": round(sum(r[3] for r in records) / len(records), 5),
               "success_rate_while_down": round(sum(r[3] for r in down) / len(down), 5) if down else None,
               "peak_hints": max((h for _, h in hint_samples), default=0),
               "hints_drained_after_restart_s": round(drained - restart_at, 2) if drained else None,
               "per_second": seconds, "hints": hint_samples}
    print(f"kill {victim} at {kill_at:.0f}s, restart at {restart_at:.0f}s: success while down "
          f"{summary['success_rate_while_down']:.2%}, peak hints {summary['peak_hints']}, "
          f"drained {summary['hints_drained_after_restart_s']}s after restart")
    return summary


def main(args) -> None:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    work = Path(".bench")
    duration = 3.0 if args.quick else args.duration
    environment = {"python": platform.python_version(), "platform": platform.platform(terse=True),
                   "cpus": os.cpu_count(), "concurrency": args.concurrency, "keys": args.keys,
                   "value_bytes": args.value_size, "duration_s": duration,
                   "date": time.strftime("%Y-%m-%d")}
    print(f"benchmarking on {environment['platform']}, {environment['cpus']} CPUs, Python {environment['python']}")
    quorum = asyncio.run(quorum_benchmark(work, duration, args.concurrency, args.keys, args.value_size))
    failure = asyncio.run(failure_benchmark(work, max(duration * 3, 9.0), args.concurrency, args.keys,
                                            args.value_size))
    (out / "benchmark.json").write_text(json.dumps({"environment": environment, "quorums": quorum,
                                                     "node_failure": failure}, indent=2))
    shutil.rmtree(work, ignore_errors=True)
    print(f"wrote {out / 'benchmark.json'}")
