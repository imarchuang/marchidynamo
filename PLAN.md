# marchidynamo — Dynamo / Cassandra-inspired leaderless KV MVP

Educational leaderless store in Go. Same spirit as other `marchi*` repos:
**one learning goal per slice**, static membership, HTTP, tests that
partition a node and still serve.

**Not Cassandra.** We borrow **consistent hashing, N/W/R, hinted handoff,
read repair, last-write-wins** — not Gossip, vnodes-by-default, CQL,
or LSM internals (you already have those in marchilogs / marchimetrics).

Pair with [marchiraft](../marchiraft): same 3-node toy cluster, opposite
consistency story.

---

## Learning goal

1. Why Dynamo has **no leader**: any node can take a write; coordinator
   fans out to replica set.
2. **Consistent hashing** (token ring) + replication factor `N`.
3. **Quorum:** `W + R > N` ⇒ read sees at least one copy of the latest write
   *if clocks / versions cooperate*.
4. **LWW (timestamp)** is a conflict rule, not consensus; concurrent writes
   can drop a value.
5. **Hinted handoff + read repair** hide temporary unavailability without
   Raft.

**Pass bar:** draw a 3-node ring, put key `K` on nodes {A,B,C} with `N=3,W=2,R=2`;
explain what happens if A is down during PUT then comes back; why this is
*not* linearizable.

---

## Concepts we keep (and drop)

| Dynamo / Cassandra | marchidynamo v0 | Deferred |
|---|---|---|
| Token ring | one token per node, static config | vnodes, virtual nodes |
| N / W / R | flags, default 3 / 2 / 2 | per-key tunable |
| Coordinator | first node that receives HTTP | preferred local coordinator |
| LWW | client or server `ts` (unix nano) | vector clocks, CRDT merge |
| Hinted handoff | store hints when replica down | hint window / replay worker |
| Read repair | GET compares replicas, push newest | anti-entropy Merkle |
| Storage | per-node `map` + JSONL WAL | LSM (reuse ideas, don’t rebuild) |
| Sloppy quorum | optional slice | hinted + sloppy together |

**Non-goals:** Gossip membership, Cassandra wide rows, lightweight
transactions (Paxos per key), counters.

---

## Core loop

```text
Client                    Coordinator (any node)              Replicas
  | PUT /kv/k  value, ts        |                                |
  +---------------------------->|  hash(k) → preference list     |
  |                             |  send to N replicas ---------->|
  |                             |<-- W successes (or hint)       |
  |  200 {ok} <-----------------+                                |
  | GET /kv/k                   |                                |
  +---------------------------->|  read R replicas, LWW merge    |
  |                             |  optional read-repair writeback|
```

---

## On-disk layout (per node)

```text
{dataDir}/
  node.json              # id, token, peers
  wal.jsonl              # {key, value, ts, origin}
  hints/
    {downNodeId}.jsonl   # writes to replay when peer returns
```

v0 memory map is source of truth after WAL replay.

---

## API

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | node id, token, peer health |
| PUT | `/kv/{key}` | body value; header `X-Ts` optional |
| GET | `/kv/{key}` | `{value, ts, replicas[]}` |
| POST | `/internal/replicate` | peer write |
| GET | `/internal/read` | peer read |
| GET | `/ring` | token map + who owns a sample key |

Flags: `-id`, `-listen`, `-token`, `-peers`, `-n -w -r`, `-dataDir`.

Default: 3 compose services `d1 d2 d3` on `8001-8003`.

---

## MVP slices

### Slice 0 — skeleton
Single node PUT/GET + WAL.

### Slice 1 — ring + routing
3 nodes, `hash(key)` → preference list; coordinator forwards.
Test: same key always maps to the same N nodes.

### Slice 2 — quorum W/R
PUT waits for W; GET waits for R, returns max(ts).
Test: `W=2` succeeds with 1 node killed; `W=3` fails.

### Slice 3 — LWW conflict
Two concurrent PUTs with different ts; GET shows winner.
Test: older PUT arriving later does not overwrite.

### Slice 4 — hinted handoff + read repair
Down replica: coordinator writes hint; on peer up, replay.
GET with divergent replicas: repair the stale one.
Test: kill d3, PUT, start d3, hint replay or GET repair → d3 catches up.

### Slice 5 — polish
`/ring` debug, docker demo, `QUORUM.md` (vs Raft table).

---

## Demo (graduation)

```bash
docker compose up
curl -X PUT d1:8001/kv/cart -d '{"n":1}'
# kill d3
curl -X PUT d1:8001/kv/cart -d '{"n":2}'    # still W=2
# start d3; GET until d3 has n=2
```

---

## Raft vs this (keep in QUORUM.md)

| | marchiraft | marchidynamo |
|---|---|---|
| Writes | one log, majority of *that log* | majority of *replica values* |
| Conflicts | uncommitted tail discarded | LWW picks a winner |
| Availability if 1 node down | yes (2/3) | yes if W,R ≤ 2 |
| Linearizable | yes (v0 leader reads) | no |

Start at **slice 0** on `feat/skeleton`.
