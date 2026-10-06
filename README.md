# marchidynamo

Educational **leaderless KV** in Go, in the spirit of Dynamo / Cassandra
replication: token ring, N/W/R quorum, last-write-wins, hinted handoff,
and read repair.

**Not Cassandra.** No Gossip, CQL, vnodes, or LSM internals.

Pair with [marchiraft](https://github.com/imarchuang/marchiraft): same
3-node toy cluster, opposite consistency story.

Defaults: **N=3, W=2, R=2**, LWW unix nanoseconds.

---

## Quick start

**Go (1.22+):**

```bash
go test ./...
go run ./cmd/marchidynamo -id=d1 -listen=:8001 -token=100 -dataDir=./data/d1
```

**Docker (3 nodes):**

```bash
docker compose up --build
curl -s -X PUT http://127.0.0.1:8001/kv/cart -d '{"n":1}'
curl -s http://127.0.0.1:8001/kv/cart
curl -s http://127.0.0.1:8001/healthz
```

The coordinator hashes the key onto a **static token ring** and forwards
PUT/GET to the N-node preference list (`/internal/replicate`, `/internal/read`).
PUT waits for **W** replica acks; GET waits for **R** replica replies and
returns the max timestamp. Defaults **N=3 W=2 R=2**.

Peers are `id=token=host:port`, e.g. `d2=200=d2:8002`.

---

## HTTP API

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | node id, token, N/W/R, peers |
| PUT | `/kv/{key}` | body is the value; optional `X-Ts` unix nano |
| GET | `/kv/{key}` | `{value, ts, origin, replicas[]}` |

Flags: `-id`, `-listen`, `-token`, `-peers`, `-n -w -r`, `-dataDir`.

---

## On-disk layout (per node)

```text
{dataDir}/
  node.json              # id, token, peers
  wal.jsonl              # {key, value, ts, origin}
```

The in-memory map is rebuilt from `wal.jsonl` on start.
