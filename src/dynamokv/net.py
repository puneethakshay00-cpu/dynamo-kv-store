"""Runs a :class:`~dynamokv.node.Node` over TCP with asyncio.

Every frame is a 4-byte big-endian length followed by a JSON envelope
``{"src": sender, "m": message}``. A node keeps one outgoing connection per peer
(reconnecting as needed) and answers clients on the connection they came in on.
Messages to an unreachable peer are dropped; the protocol already copes with loss
through timeouts, fallbacks and anti-entropy.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import struct
from collections.abc import Callable

from .config import Config
from .messages import decode, encode
from .node import Node

log = logging.getLogger("dynamokv")

HEADER = struct.Struct(">I")
MAX_FRAME = 64 * 1024 * 1024


def frame(envelope: dict) -> bytes:
    payload = encode(envelope)
    return HEADER.pack(len(payload)) + payload


async def read_frame(reader: asyncio.StreamReader) -> dict | None:
    try:
        (size,) = HEADER.unpack(await reader.readexactly(HEADER.size))
        if size > MAX_FRAME:
            raise ValueError(f"frame of {size} bytes is too large")
        return decode(await reader.readexactly(size))
    except (asyncio.IncompleteReadError, ConnectionError):
        return None


class _Runtime:
    def __init__(self, server: NodeServer) -> None:
        self.server = server
        self.rng = random.Random()

    def now(self) -> float:
        return self.server.loop.time()

    def send(self, dst: str, message: dict) -> None:
        self.server.send(dst, message)

    def call_later(self, delay: float, fn: Callable[[], None]) -> asyncio.TimerHandle:
        return self.server.loop.call_later(delay, self.server.guarded, fn)


class _Peer:
    """Outgoing connection to one peer, fed from a bounded queue."""

    def __init__(self, server: NodeServer, host: str, port: int) -> None:
        self.server, self.host, self.port = server, host, port
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=50_000)
        self.task = server.loop.create_task(self._run())

    def send(self, envelope: dict) -> None:
        with contextlib.suppress(asyncio.QueueFull):
            self.queue.put_nowait(envelope)

    async def _run(self) -> None:
        while True:
            writer = None
            try:
                _, writer = await asyncio.wait_for(asyncio.open_connection(self.host, self.port), timeout=1.0)
                while True:
                    writer.write(frame(await self.queue.get()))
                    while not self.queue.empty():
                        writer.write(frame(self.queue.get_nowait()))
                    await writer.drain()
            except (OSError, asyncio.TimeoutError):
                while not self.queue.empty():      # stale by the time the peer is back
                    self.queue.get_nowait()
                await asyncio.sleep(0.1)
            finally:
                if writer is not None:
                    writer.close()


class NodeServer:
    def __init__(self, node_id: str, addresses: dict[str, tuple[str, int]], config: Config, storage) -> None:
        self.id = node_id
        self.addresses = {name: (host, int(port)) for name, (host, port) in addresses.items()}
        self.config = config
        self.storage = storage
        self.clients: dict[str, asyncio.StreamWriter] = {}
        self.connections: set[asyncio.StreamWriter] = set()
        self.peers: dict[str, _Peer] = {}
        self.node: Node | None = None
        self._server: asyncio.base_events.Server | None = None

    async def start(self) -> None:
        self.loop = asyncio.get_running_loop()
        host, port = self.addresses[self.id]
        self._server = await asyncio.start_server(self._accept, host, port)
        self.peers = {name: _Peer(self, *addr) for name, addr in self.addresses.items() if name != self.id}
        self.node = Node(self.id, list(self.addresses), self.config, self.storage, _Runtime(self))
        self.node.start()
        log.info("%s listening on %s:%s", self.id, host, port)

    async def serve_forever(self) -> None:
        await self.start()
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self.node is not None:
            self.node.stop()
        for peer in self.peers.values():
            peer.task.cancel()
        if self._server is not None:
            self._server.close()
        for writer in list(self.connections):   # wait_closed() waits for open connections
            writer.close()
        if self._server is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self._server.wait_closed(), timeout=2.0)

    def guarded(self, fn: Callable, *args) -> None:
        try:
            fn(*args)
        except Exception:  # one bad message must not take the node down
            log.exception("%s: error while handling an event", self.id)

    def send(self, dst: str, message: dict) -> None:
        envelope = {"src": self.id, "m": message}
        if dst in self.peers:
            self.peers[dst].send(envelope)
        elif dst in self.clients:
            self.clients[dst].write(frame(envelope))

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        src = None
        self.connections.add(writer)
        try:
            while (envelope := await read_frame(reader)) is not None:
                src = envelope["src"]
                if src not in self.addresses:
                    self.clients[src] = writer
                if self.node is not None:
                    self.guarded(self.node.handle, src, envelope["m"])
        finally:
            self.connections.discard(writer)
            if src is not None and self.clients.get(src) is writer:
                del self.clients[src]
            writer.close()
