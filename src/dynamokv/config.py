"""Cluster settings. Times are in seconds."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from .version import POLICIES


@dataclass(frozen=True)
class Config:
    n: int = 3                       # replicas per key
    r: int = 2                       # replies a read waits for
    w: int = 2                       # acknowledgements a write waits for
    vnodes: int = 32                 # tokens per node on the hash ring
    clock: str = "dvv"               # "dvv" (dotted version vectors) or "vclock" (the paper's clocks)
    fallback_after: float = 0.08     # wait this long for replies before trying fallback nodes
    request_timeout: float = 0.5     # give up on a client request after this long
    gossip_interval: float = 0.1
    suspect_after: float = 0.6       # no sign of life for this long: treat a peer as down
    hint_interval: float = 0.25      # how often to try delivering hinted replicas
    handoff_batch: int = 256
    anti_entropy_interval: float = 0.5
    read_repair: bool = True
    sloppy_quorum: bool = True       # False: only the N preferred nodes may hold a key (strict quorum)

    def __post_init__(self) -> None:
        if not 1 <= self.r <= self.n or not 1 <= self.w <= self.n:
            raise ValueError("need 1 <= R <= N and 1 <= W <= N")
        if self.clock not in POLICIES:
            raise ValueError(f"clock must be one of {sorted(POLICIES)}")

    def as_dict(self) -> dict:
        return asdict(self)
