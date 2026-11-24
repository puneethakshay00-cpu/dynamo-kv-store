"""A narrated tour on a real 5-node TCP cluster running inside one process."""

from __future__ import annotations

import asyncio
import socket
from pathlib import Path

from .client import Client
from .config import Config
from .net import NodeServer
from .storage import FileStorage
from .version import POLICIES


def _free_ports(count: int) -> list[int]:
    sockets = [socket.socket() for _ in range(count)]
    for s in sockets:
        s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in sockets]
    for s in sockets:
        s.close()
    return ports


def _say(step: str, text: str = "") -> None:
    print(f"\n== {step}" if text == "" else f"   {text}")


async def _wait_for(condition, timeout: float = 10.0) -> float:
    loop = asyncio.get_running_loop()
    start = loop.time()
    while not condition():
        if loop.time() - start > timeout:
            raise TimeoutError("condition not reached")
        await asyncio.sleep(0.05)
    return loop.time() - start


async def run_demo(directory: Path) -> None:
    config = Config()
    policy = POLICIES[config.clock]
    names = [f"n{i}" for i in range(1, 6)]
    addresses = {name: ("127.0.0.1", port) for name, port in zip(names, _free_ports(len(names)), strict=True)}
    servers = {name: NodeServer(name, addresses, config, FileStorage(policy, directory / name)) for name in names}
    for server in servers.values():
        await server.start()

    def hints() -> int:
        return sum(s.storage.hint_count() for s in servers.values() if s.node and not s.node.stopped)

    try:
        async with Client(addresses, config, timeout=1.0) as alice, Client(addresses, config, timeout=1.0) as bob:
            key = "cart:42"
            replicas = alice.ring.preference_list(key, config.n)
            _say(f"5 nodes up, N={config.n} R={config.r} W={config.w}")
            _say("", f"'{key}' lives on {', '.join(replicas)} (the first {config.n} nodes clockwise on the ring)")

            _say("Write and read back")
            dot = await alice.put(key, b"book")
            result = await alice.get(key)
            _say("", f"put 'book' -> version {dot[0]}@{dot[1]}; get -> {result.values}")

            _say("Two clients update the same version concurrently")
            await asyncio.gather(alice.put(key, b"book, pen", result.context),
                                 bob.put(key, b"book, mug", result.context))
            both = await alice.get(key)
            _say("", f"get -> {sorted(both.values)}  (siblings: neither write is silently dropped)")
            await alice.put(key, b"book, mug, pen", both.context)
            _say("", f"a write that merges them, sent with the joint context -> {(await alice.get(key)).values}")

            victim = replicas[1]
            _say(f"Kill {victim}, one of the key's replicas")
            await servers[victim].stop()
            servers[victim].storage.close()
            await asyncio.sleep(config.suspect_after)
            await alice.put(key, b"book, mug, pen, lamp")
            holders = [n for n, s in servers.items() if n != victim and s.storage.hint_count()]
            _say("", f"put still succeeds (sloppy quorum); {', '.join(holders)} keeps a hinted copy for {victim}")
            _say("", f"get -> {(await alice.get(key)).values}")

            _say(f"Restart {victim} from its write-ahead log")
            servers[victim] = NodeServer(victim, addresses, config, FileStorage(policy, directory / victim))
            await servers[victim].start()
            waited = await _wait_for(lambda: hints() == 0)
            stored = [v.value for v in servers[victim].storage.get(key)]
            _say("", f"hinted handoff delivered the missed write after {waited:.2f}s; {victim} now has {stored}")
            print(f"\nData directory: {directory}")
    finally:
        for server in servers.values():
            await server.stop()
