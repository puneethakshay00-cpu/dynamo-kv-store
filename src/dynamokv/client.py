"""Async client library.

The client knows the ring, so it sends each put straight to a replica of the key, the
way Dynamo's partition-aware client library does, and fails over to the next replica
if one does not answer. Reads can go to any node.

Values are bytes. A get returns every concurrent value (siblings) plus a *context*;
pass the context back with the next put so the store knows which versions it replaces.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import uuid
from dataclasses import dataclass, field

from .config import Config
from .messages import GET, PUT, STATS
from .net import frame, read_frame
from .ring import Ring


class Unavailable(Exception):
    """No node could serve the request."""


@dataclass
class GetResult:
    values: list[bytes]                 # concurrent values; empty if the key is missing or deleted
    context: dict[str, int]
    versions: list = field(default_factory=list)

    @property
    def value(self) -> bytes | None:
        """The value if there is exactly one, else None."""
        return self.values[0] if len(self.values) == 1 else None


class _Connection:
    def __init__(self, reader, writer, client: Client) -> None:
        self.writer = writer
        self.task = asyncio.get_running_loop().create_task(self._read(reader, client))

    async def _read(self, reader, client: Client) -> None:
        while (envelope := await read_frame(reader)) is not None:
            future = client._pending.pop(envelope["m"].get("req"), None)
            if future is not None and not future.done():
                future.set_result(envelope["m"])

    def close(self) -> None:
        self.task.cancel()
        self.writer.close()


class Client:
    def __init__(self, addresses: dict[str, tuple[str, int]], config: Config = Config(),
                 timeout: float = 2.0) -> None:
        self.addresses = {name: (host, int(port)) for name, (host, port) in addresses.items()}
        self.config = config
        self.ring = Ring(self.addresses, config.vnodes)
        self.timeout = timeout
        self.id = f"client-{uuid.uuid4().hex[:12]}"
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future] = {}
        self._connections: dict[str, _Connection] = {}
        self._down: set[str] = set()

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def close(self) -> None:
        for connection in self._connections.values():
            connection.close()
        self._connections.clear()

    async def _connection(self, node: str) -> _Connection:
        connection = self._connections.get(node)
        if connection is None or connection.task.done():
            reader, writer = await asyncio.wait_for(asyncio.open_connection(*self.addresses[node]), self.timeout)
            connection = self._connections[node] = _Connection(reader, writer, self)
        return connection

    async def _call(self, node: str, message: dict) -> dict:
        req = next(self._ids)
        message["req"] = req
        future = asyncio.get_running_loop().create_future()
        self._pending[req] = future
        try:
            connection = await self._connection(node)
            connection.writer.write(frame({"src": self.id, "m": message}))
            return await asyncio.wait_for(future, self.timeout)
        except (OSError, asyncio.TimeoutError):
            self._down.add(node)
            stale = self._connections.pop(node, None)
            if stale is not None:
                stale.close()
            raise
        finally:
            self._pending.pop(req, None)

    def _order(self, nodes) -> list[str]:
        """Nodes that have not failed us recently go first."""
        return sorted(nodes, key=lambda n: n in self._down)

    async def _first_success(self, nodes, message: dict) -> dict:
        errors = []
        for node in self._order(nodes):
            with contextlib.suppress(OSError, asyncio.TimeoutError):
                reply = await self._call(node, dict(message))
                self._down.discard(node)
                if reply.get("ok"):
                    return reply
                errors.append(f"{node}: {reply.get('error')}")
                continue
            errors.append(f"{node}: no answer")
        raise Unavailable("; ".join(errors))

    async def get(self, key: str) -> GetResult:
        preference = self.ring.preference_list(key, self.config.n)
        others = [n for n in self.ring.nodes if n not in preference]
        reply = await self._first_success([*preference, *others], {"t": GET, "key": key})
        values = [v.value for v in reply["versions"] if v.value is not None]
        return GetResult(values, reply["ctx"], reply["versions"])

    async def put(self, key: str, value: bytes | None, context: dict | None = None) -> tuple[str, int]:
        """Write ``value`` (None deletes). Without a context the client reads first, so the
        write replaces whatever is there now."""
        if context is None:
            context = (await self.get(key)).context
        message = {"t": PUT, "key": key, "value": value, "ctx": context}
        reply = await self._first_success(self.ring.preference_list(key, self.config.n), message)
        return tuple(reply["dot"])

    async def delete(self, key: str, context: dict | None = None) -> tuple[str, int]:
        return await self.put(key, None, context)

    async def stats(self, node: str) -> dict:
        return await self._call(node, {"t": STATS})
