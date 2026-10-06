# Quorum vs Raft

marchidynamo is **leaderless**. Any node may coordinate a request: hash the
key, fan out to the preference list, and wait for W writes or R reads.
That is a majority of *replica values*, not a majority of a single replicated
log.

`W + R > N` means a read overlaps at least one replica that took the last
successful write — **if** timestamps agree. Last-write-wins is a conflict
rule, not consensus. Concurrent PUTs can drop a value; clocks can lie;
the result is **not linearizable**.

Hinted handoff and read repair hide a temporarily down replica. They do not
create a committed history the way Raft does.

| | marchiraft | marchidynamo |
|---|---|---|
| Writes | one log, majority of *that log* | majority of *replica values* |
| Conflicts | uncommitted tail discarded | LWW picks a winner |
| Availability if 1 node down | yes (2/3) | yes if W,R ≤ 2 |
| Linearizable | yes (v0 leader reads) | no |

## Draw the 3-node ring

Default tokens: d1=100, d2=200, d3=300. For `N=3, W=2, R=2` every key’s
preference list is all three nodes (starting at different places).

1. PUT `K` while A (say d3) is down: coordinator writes B and C, stores a
   **hint** for A, returns 200 because W=2.
2. A comes back empty. Hint replay and/or a later GET **read-repair** push
   the LWW winner onto A.
3. Why this is not linearizable: a client can PUT x, PUT y (overlapping
   coordinators, skewed `X-Ts`), and a later GET can show x, or lose y,
   without a single cluster-wide order.

Pair with [marchiraft](https://github.com/imarchuang/marchiraft) for the
opposite story: one leader, one log, majority commit.
