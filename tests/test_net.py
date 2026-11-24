"""The same node code over real TCP sockets."""

import asyncio
import json
import socket
import subprocess
import sys

from dynamokv.client import Client
from dynamokv.config import Config
from dynamokv.net import NodeServer
from dynamokv.storage import FileStorage
from dynamokv.version import DottedVersionVectors


def free_ports(count):
    sockets = [socket.socket() for _ in range(count)]
    for s in sockets:
        s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in sockets]
    for s in sockets:
        s.close()
    return ports


def test_tcp_cluster_survives_a_node_restart(tmp_path):
    async def scenario():
        config = Config()
        addresses = {f"n{i}": ("127.0.0.1", p) for i, p in enumerate(free_ports(5), start=1)}
        servers = {n: NodeServer(n, addresses, config, FileStorage(DottedVersionVectors, tmp_path / n))
                   for n in addresses}
        for server in servers.values():
            await server.start()
        try:
            async with Client(addresses, config, timeout=1.0) as client:
                await client.put("k", b"one")
                assert (await client.get("k")).values == [b"one"]

                victim = client.ring.preference_list("k", 3)[0]
                await servers[victim].stop()
                servers[victim].storage.close()
                await asyncio.sleep(config.suspect_after)
                await client.put("k", b"two")                     # fails over to another replica
                assert (await client.get("k")).values == [b"two"]

                servers[victim] = NodeServer(victim, addresses, config,
                                             FileStorage(DottedVersionVectors, tmp_path / victim))
                await servers[victim].start()
                for _ in range(100):
                    if [v.value for v in servers[victim].storage.get("k")] == [b"two"]:
                        break
                    await asyncio.sleep(0.05)
                assert [v.value for v in servers[victim].storage.get("k")] == [b"two"]
        finally:
            for server in servers.values():
                await server.stop()

    asyncio.run(scenario())


def test_cli_cluster(tmp_path):
    base = free_ports(1)[0]
    cluster = tmp_path / "cluster.json"

    def run(*args):
        return subprocess.run([sys.executable, "-m", "dynamokv", *args], capture_output=True, text=True, timeout=60)

    up = run("cluster", "up", "--nodes", "3", "--base-port", str(base), "--dir", str(tmp_path))
    assert up.returncode == 0, up.stderr
    try:
        assert json.loads(cluster.read_text())["nodes"]["n1"][1] == base + 1
        for _ in range(20):
            put = run("put", "greeting", "hello", "--cluster", str(cluster))
            if put.returncode == 0:
                break
        assert put.returncode == 0, put.stderr
        get = run("get", "greeting", "--cluster", str(cluster))
        assert get.stdout.strip() == "hello"
    finally:
        run("cluster", "down", "--dir", str(tmp_path))
