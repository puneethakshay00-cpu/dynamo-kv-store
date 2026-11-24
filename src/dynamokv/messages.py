"""Message types and the JSON wire codec.

Messages are plain dicts with a ``"t"`` type field. Inside the simulator they are passed
as Python objects; over TCP they are encoded as JSON with versions and raw bytes tagged.
"""

from __future__ import annotations

import base64
import json

from .storage import decode_version, encode_version
from .version import Version

# client <-> any node
GET, GET_REPLY = "get", "get_reply"
PUT, PUT_REPLY = "put", "put_reply"
STATS, STATS_REPLY = "stats", "stats_reply"

# node <-> node
REPLICATE, REPLICATE_ACK = "replicate", "replicate_ack"
READ, READ_REPLY = "read", "read_reply"
REPAIR = "repair"
HANDOFF, HANDOFF_ACK = "handoff", "handoff_ack"
GOSSIP = "gossip"
AE_SYNC, AE_DIFF, AE_LEAVES, AE_DATA = "ae_sync", "ae_diff", "ae_leaves", "ae_data"


def _default(obj):
    if isinstance(obj, Version):
        return {"$v": encode_version(obj)}
    if isinstance(obj, bytes):
        return {"$b": base64.b64encode(obj).decode("ascii")}
    if isinstance(obj, (set, frozenset)):
        return list(obj)
    raise TypeError(f"cannot encode {type(obj).__name__}")


def _hook(obj: dict):
    if "$v" in obj:
        return decode_version(obj["$v"])
    if "$b" in obj:
        return base64.b64decode(obj["$b"])
    return obj


def encode(message: dict) -> bytes:
    return json.dumps(message, default=_default, separators=(",", ":")).encode("utf-8")


def decode(data: bytes) -> dict:
    return json.loads(data, object_hook=_hook)
