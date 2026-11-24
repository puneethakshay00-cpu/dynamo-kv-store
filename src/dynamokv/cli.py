"""Command line: run nodes, start a local cluster, read and write keys, run the
simulator, benchmark.

    dynamokv demo                      # 5-node cluster in one process, narrated
    dynamokv cluster up --nodes 5      # 5 node processes on localhost
    dynamokv put cart '{"items": 2}'
    dynamokv get cart
    dynamokv cluster down
    dynamokv simulate --seeds 1000     # fault-injection scenarios, fails on any violation
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from .config import Config

DEFAULT_DIR = Path(".cluster")


# ---------------------------------------------------------------- cluster files
def write_cluster_file(directory: Path, nodes: int, base_port: int, config: Config, host: str = "127.0.0.1") -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    spec = {"nodes": {f"n{i}": [host, base_port + i] for i in range(1, nodes + 1)}, "config": config.as_dict()}
    path = directory / "cluster.json"
    path.write_text(json.dumps(spec, indent=2))
    return path


def load_cluster(path: Path) -> tuple[dict, Config]:
    spec = json.loads(Path(path).read_text())
    return {name: tuple(addr) for name, addr in spec["nodes"].items()}, Config(**spec["config"])


def _cluster_path(args) -> Path:
    return Path(args.cluster) if args.cluster else DEFAULT_DIR / "cluster.json"


# ---------------------------------------------------------------- commands
def cmd_node(args) -> None:
    from .net import NodeServer
    from .storage import FileStorage
    from .version import POLICIES

    addresses, config = load_cluster(Path(args.cluster))
    data = Path(args.data) if args.data else Path(args.cluster).parent / args.id
    storage = FileStorage(POLICIES[config.clock], data, fsync=args.fsync)
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s {args.id} %(message)s")
    asyncio.run(NodeServer(args.id, addresses, config, storage).serve_forever())


def start_node_process(cluster: Path, node: str, log_dir: Path) -> subprocess.Popen:
    log = open(log_dir / f"{node}.log", "ab")  # noqa: SIM115 - handed to the child process
    return subprocess.Popen([sys.executable, "-m", "dynamokv", "node", "--id", node, "--cluster", str(cluster)],
                            stdout=log, stderr=subprocess.STDOUT)


def cmd_cluster(args) -> None:
    directory = Path(args.dir)
    pid_file = directory / "pids.json"
    if args.action == "up":
        config = Config(n=args.n, r=args.r, w=args.w, clock=args.clock, sloppy_quorum=not args.strict)
        cluster = write_cluster_file(directory, args.nodes, args.base_port, config)
        pids = {}
        for node in load_cluster(cluster)[0]:
            pids[node] = start_node_process(cluster, node, directory).pid
        pid_file.write_text(json.dumps(pids))
        time.sleep(0.5)
        print(f"started {len(pids)} nodes ({config.n=}, {config.r=}, {config.w=}) - cluster file {cluster}")
    elif args.action == "down":
        pids = json.loads(pid_file.read_text()) if pid_file.exists() else {}
        for pid in pids.values():
            with contextlib.suppress(ProcessLookupError, OSError):
                os.kill(pid, signal.SIGTERM)
        pid_file.unlink(missing_ok=True)
        print(f"stopped {len(pids)} nodes")
    else:
        asyncio.run(_status(directory / "cluster.json"))


async def _status(cluster: Path) -> None:
    from .client import Client

    addresses, config = load_cluster(cluster)
    async with Client(addresses, config, timeout=1.0) as client:
        for node in addresses:
            try:
                s = await client.stats(node)
                print(f"{node}: up   keys={s['keys']:<6} hints={s['hints']:<4} sees up: {', '.join(s['up'])}")
            except (OSError, asyncio.TimeoutError):
                print(f"{node}: DOWN")


def cmd_kv(args) -> None:
    from .client import Client

    async def run() -> None:
        addresses, config = load_cluster(_cluster_path(args))
        async with Client(addresses, config) as client:
            if args.command == "get":
                result = await client.get(args.key)
                if not result.values:
                    print("(not found)")
                for value in result.values:
                    print(value.decode(errors="replace"))
                if len(result.values) > 1:
                    print(f"({len(result.values)} concurrent values; the next put resolves them)")
            elif args.command == "put":
                print("stored as", "@".join(map(str, await client.put(args.key, args.value.encode()))))
            else:
                await client.delete(args.key)
                print("deleted")

    asyncio.run(run())


def cmd_simulate(args) -> int:
    from .experiments import sweep

    config = Config(n=args.n, r=args.r, w=args.w, clock=args.clock, sloppy_quorum=not args.strict)
    reports = sweep(range(args.start, args.start + args.seeds), config, faults=not args.no_faults,
                    workers=args.workers)
    failed = [r for r in reports if not r.ok]
    for report in failed[:20]:
        print(report.summary())
    acked = sum(r.writes_ok for r in reports)
    lost = sum(len(r.lost_writes) for r in reports)
    reads = sum(r.reads_ok for r in reports)
    stale = sum(r.stale_reads for r in reports)
    print(f"{len(reports)} scenarios, {acked:,} acknowledged writes, {lost} lost, "
          f"{stale:,} stale reads of {reads:,}, {len(failed)} failed")
    if args.json:
        Path(args.json).write_text(json.dumps([r.as_dict() for r in reports]))
    return 1 if failed else 0


def cmd_demo(args) -> None:
    from .demo import run_demo

    asyncio.run(run_demo(Path(args.dir) if args.dir else Path(tempfile.mkdtemp(prefix="dynamokv-demo-"))))


def cmd_bench(args) -> None:
    from .bench import main as bench_main

    bench_main(args)


# ---------------------------------------------------------------- parser
def _quorum_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("-n", type=int, default=3, help="replicas per key")
    p.add_argument("-r", type=int, default=2, help="read quorum")
    p.add_argument("-w", type=int, default=2, help="write quorum")
    p.add_argument("--clock", default="dvv", choices=["dvv", "vclock"])
    p.add_argument("--strict", action="store_true", help="strict quorum: no fallback nodes")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dynamokv", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("node", help="run one node in the foreground")
    p.add_argument("--id", required=True)
    p.add_argument("--cluster", required=True)
    p.add_argument("--data", help="data directory (default: next to the cluster file)")
    p.add_argument("--fsync", action="store_true", help="fsync the log on every write")
    p.set_defaults(func=cmd_node)

    p = sub.add_parser("cluster", help="start, stop or inspect a local cluster")
    p.add_argument("action", choices=["up", "down", "status"])
    p.add_argument("--nodes", type=int, default=5)
    p.add_argument("--base-port", type=int, default=7100)
    p.add_argument("--dir", default=str(DEFAULT_DIR))
    _quorum_args(p)
    p.set_defaults(func=cmd_cluster)

    for name in ("get", "put", "delete"):
        p = sub.add_parser(name, help=f"{name} a key")
        p.add_argument("key")
        if name == "put":
            p.add_argument("value")
        p.add_argument("--cluster", help="cluster file (default .cluster/cluster.json)")
        p.set_defaults(func=cmd_kv)

    p = sub.add_parser("simulate", help="run seeded fault-injection scenarios through the checker")
    p.add_argument("--seeds", type=int, default=200)
    p.add_argument("--start", type=int, default=0, help="first seed")
    p.add_argument("--workers", type=int, default=os.cpu_count() or 1)
    p.add_argument("--no-faults", action="store_true")
    p.add_argument("--json", help="write every report to this file")
    _quorum_args(p)
    p.set_defaults(func=cmd_simulate)

    p = sub.add_parser("demo", help="narrated walk-through on an in-process 5-node cluster")
    p.add_argument("--dir", help="where to keep the nodes' data (default: a temp directory)")
    p.set_defaults(func=cmd_demo)

    p = sub.add_parser("bench", help="benchmark a local multi-process cluster")
    p.add_argument("--duration", type=float, default=10.0)
    p.add_argument("--concurrency", type=int, default=32)
    p.add_argument("--keys", type=int, default=10_000)
    p.add_argument("--value-size", type=int, default=256)
    p.add_argument("--out", default="results")
    p.add_argument("--quick", action="store_true", help="short runs, for a smoke test")
    p.set_defaults(func=cmd_bench)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = args.func(args)
    return result if isinstance(result, int) else 0


if __name__ == "__main__":
    sys.exit(main())
