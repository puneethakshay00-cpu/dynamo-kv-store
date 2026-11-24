# Design

This store follows the design in *Dynamo: Amazon's Highly Available Key-value Store*
(DeCandia et al., SOSP 2007). This document explains how each part works here, where it
differs from the paper, and why.

## Data model and API

Keys are strings and values are bytes. Three operations:

| Operation | Meaning |
|---|---|
| `get(key)` | Every concurrent value of the key (its *siblings*) and a *context*. |
| `put(key, value, context)` | Write a value. The context says which versions the client had read, so they are replaced. |
| `delete(key, context)` | A put of a tombstone, so the delete is versioned like any other write. |

As in Dynamo, a get can return more than one value when writes were concurrent. The
application decides how to merge them (a shopping cart would take the union) and writes
the result back with the context it received, which retires the siblings.

## Partitioning: consistent hashing

[`ring.py`](../src/dynamokv/ring.py). Each node owns 32 tokens on a 64-bit ring (MD5 of
`node#i`). A key is stored on the first `N` distinct nodes clockwise from its hash: its
*preference list*. Virtual nodes keep the load even: with 5 nodes, each is the first
replica for 15.6% to 23.7% of the ring (20% would be perfect), and 64 tokens per node
narrows that to 17% to 22%. They also make growth cheap: adding a sixth node moves about
a fifth of the keys (ideally a sixth), not most of them.

## Replication and quorums

[`node.py`](../src/dynamokv/node.py). With `N` replicas per key, a write is acknowledged
once `W` replicas have stored it and a read returns once `R` replicas answered.

* **Writes** are coordinated by a replica of the key (the partition-aware client sends
  them there; any other node answers with a redirect). The coordinator creates the new
  version, merges it into its own copy and sends the key's **whole version set** to the
  other replicas. It replies to the client after `W` acknowledgements, itself included.
* **Reads** can be coordinated by any node. It asks the first `N` healthy nodes on the
  ring, waits for `R` replies, reconciles them and returns all surviving siblings plus
  the joint context.
* Requests are sent to all `N` replicas whatever `R` and `W` are; the quorum sizes only
  decide when to answer. This is why the benchmark's throughput barely changes with them.

## Versioning: dotted version vectors

[`version.py`](../src/dynamokv/version.py). Every write gets a *dot*, `(coordinator,
counter)`, and carries the client's context, a version vector of everything it had read.
Version A supersedes version B only if A's context covers B's dot. The counter is a
per-node counter stored durably in the write-ahead log, so a node never issues the same
dot twice, even across crashes.

**Why not the paper's vector clocks?** In the paper, the coordinator creates the new
clock by incrementing its own entry in the clock the client sent. Two clients that read
the same version and write through the same coordinator both produce the clock
`{n1: k+1}`; the replicas see one version twice and keep only one value, even though
both writes were acknowledged. A client whose context is out of date can even produce a
clock that looks *older* than what the replica has, and that write is dropped too. The
simulator reproduces both cases in every run (see [testing.md](testing.md)); the store
keeps the paper's scheme behind `--clock vclock` so this can be shown.

**The invariant that makes the context safe.** A version vector `{n1: 7}` claims that the
client has seen every `n1` event up to 7. That is only true if no replica can hold dot
`(n1, 7)` without also holding `(n1, 5)` or something that superseded it. Two rules
guarantee it:

1. Only a replica of the key coordinates writes, so `n1` always holds every version it
   issued for that key (until one is superseded).
2. Replication, hinted handoff, read repair and anti-entropy always move a key's whole
   version set, never a single version.

## Sloppy quorum and hinted handoff

When a preferred replica looks down, the coordinator sends its copy to the next healthy
node on the ring instead. That node keeps it as a *hint* for the intended replica, in a
separate area of its storage, and still includes it when answering reads. Every 250 ms
each node tries to hand its hints back to their owners. A hint is deleted only after the
owner acknowledges it, and only the versions that were actually delivered are removed.

The cost is consistency. Under partitions, two clients can write through different
sides, and a read that reaches a side without the latest write returns stale data. With
`--strict` (no fallback nodes) the store refuses those writes instead. The simulator
measures both behaviours.

If a replica has not acknowledged within 80 ms, the coordinator marks it suspect and
retries on the next fallback node. A request that has not reached its quorum after
500 ms fails.

## Read repair

After answering, the read coordinator keeps waiting for the other replicas. Any
preferred replica whose versions differ from the reconciled result is sent the
reconciled set.

## Anti-entropy with Merkle trees

[`merkle.py`](../src/dynamokv/merkle.py). Every 500 ms each node picks a random live peer
and compares the keys they both replicate. Both build a binary tree with 64 leaves: each
leaf hashes the keys in its bucket with their version ids, and each inner node hashes
its two children. The initiator sends its root. The peer answers with the indices that
differ and the initiator sends the next level for those subtrees only. At the leaves the
two exchange the versions of the keys in the differing buckets and both merge them.
Replicas that are in sync need one round trip. A pair with one differing key needs one
round trip per tree level (seven) and exchanges the data of a single bucket.

## Failure detection: gossip

Every 100 ms a node bumps its heartbeat, `(epoch, counter)`, and sends its whole
heartbeat table to a random peer. The epoch is a durable incarnation number, so a
restarted node's heartbeats count as new. A peer is considered up if its heartbeat
advanced, or any message arrived from it, within the last 600 ms. A peer that missed a
request deadline is suspected immediately, until it is heard from directly. Membership
itself is static: the cluster file lists the nodes, as with Dynamo's explicit membership
changes.

## Durability

[`storage.py`](../src/dynamokv/storage.py). Each node appends every change (a key's new
version set, a hint, the write counter and the epoch) to a JSON-lines write-ahead log
before acknowledging, and replays it on start. A torn final record from a crash is
ignored. By default the log is flushed to the OS on every write; `--fsync` forces it to
disk as well, trading throughput for surviving power loss.

## One codebase, two runtimes

The node is a state machine: it reacts to messages and timers and never blocks or
touches a socket. It talks to the world through four functions: `send`, `call_later`,
`now` and `rng`.

* [`sim.py`](../src/dynamokv/sim.py) implements them with a virtual clock and one seeded
  random generator, which makes every run reproducible.
* [`net.py`](../src/dynamokv/net.py) implements them with asyncio and TCP:
  length-prefixed JSON frames, one outgoing connection per peer and replies to clients
  on their own connection.

So the code that the simulator tests through thousands of failures is the same code that
serves real traffic.

## Not implemented

* **Adding and removing nodes at runtime.** The ring is static. Rebalancing would need
  key transfer and a membership protocol.
* **Garbage collection.** Tombstones are kept forever, and the write-ahead log is never
  compacted.
* **Security and backpressure.** There is no authentication, and no limit on in-flight
  requests per client.
