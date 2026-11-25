# Author shards on production: when, and how

The runbook for splitting production (search.brainstorm.world) by author, written against its
real deployment: the `vespa-relay` chart in NosFabrica/brainstorm-k8s, namespace
`vespa-eventstore-staging` on brainstormk2. Every number below is from that chart's values
comments or from the team's own figures (2026-11), not measured here. The design and the tools
are in [author-shards.md](author-shards.md); this page is the order of operations and the
arithmetic.

## Where production is

- **Five content nodes, one copy of everything.** `vespa.nodes: 5` (pods `vespa-0`..`vespa-4`,
  `vespa-4` on brainstormk6), `<redundancy>1</redundancy>`: each document lives on exactly one
  node. The package is the chart's own (a Helm hook Job renders `services.xml`, overlays the
  store's schemas from the relay image, and POSTs it to the config server); `AUTO_DEPLOY` is
  off.
- **Memory-bound.** Each Vespa pod has a 100 GiB limit on a ~125 GiB host. Proton read
  0.74-0.87 of it when the fifth node was added (2026-11-11), after four nodes were "back at the
  edge in nine days" and three were OOM-killed on 2026-11-08.
- **The trust copy is ~5% of it.** Per node: ~189 M events in ~77.5 GiB, and all 767,983
  reputation documents in ~4.4 GiB (the same set on every node), plus ~38 GiB of reputation disk,
  most likely transaction log (author-shards.md, "The trust copy's size").

## What a split buys, and what it does not

**The split shrinks only the reputation copy.** Each node keeps the reputation documents of its
own cluster's authors instead of all of them. Events are already divided across the five nodes:
adding nodes relieves them whether or not authors are split.

Worked through for production plus two new hosts, as `content` (nodes 0-4) and `content1`
(5-6), with `content1` taking 74 of 256 buckets (`b6-ff`, ~2/7 of authors, so every node ends
near the same event count):

| | events per node | reputation per node |
|---|---|---|
| today, 5 nodes, one cluster | ~189 M | ~4.4 GiB |
| 7 nodes, one cluster (`vespa.nodes: 7`, no split) | ~135 M | ~4.4 GiB |
| 7 nodes, split 5 + 2 | ~134 M (`content`), ~137 M (`content1`) | ~3.1 GiB, ~1.3 GiB |

So for today's numbers, **adding nodes to the one cluster gets nearly everything a split
does**, with no bucket move, no new settings, and no client change. The split saves another
1.3-3.1 GiB per node: real, but small next to the 100 GiB limit. It becomes the lever when the
reputation share does, as customers multiply the trust assertions every node must hold.
`StoreTelemetry.headroom` reports that share per node (`peakTrustMemoryShare`). Revisit the
split when it passes roughly a fifth of a node's document memory, or when the reputation copy
alone would not fit a node.

## Why the split needs new hosts

A split needs nodes for the new cluster. Production's own five cannot be them:

- **A node holding data cannot leave its cluster in one deploy.** At redundancy 1 it is the only
  copy of what it holds, so the deploy deletes that share. The store refuses it twice: the
  splitter (`VespaApp.shardServices`) keeps every live node in `content`, and the deploy guard
  refuses a package that drops a node the live package does not mark `retired="true"`.
- **Retiring two of the five onto the other three does not fit.** Three nodes would have to hold
  all five's documents, ~1.7x their load, on pods already at ~0.8 of their limit.
- **Running both clusters on the same hosts during the move** doubles the moving buckets' memory
  until cleanup, on hosts with no room for it. More clusters than hosts also costs throughput
  (measured: 9% at 2 clusters on one host; author-shards.md).

So the split is: **new hosts for the new cluster, then move a share of buckets onto them.** The
old nodes never take on extra data, and after cleanup they hold less.

## The steps

Nothing here is to be run until the prerequisites hold. Every step is reversible until step 6's
cleanup, which deletes the moved documents from `content` once they are verified in `content1`.

0. **Prerequisites.**
   - A store release with author shards (vespa-eventstore #221) pinned in vespa-relay, and
     vespa-relay's branch with the reindex-cursor fix and the documented settings.
   - The brainstorm-k8s chart able to render the sharded package: content nodes 0-4 in
     `content`, new pods 5-6 in `content1`, `hosts.xml` naming all seven, and the selections and
     bucket-map comment. That is `shardPackage`'s output for the chart's own `services.xml`;
     checked against production's real file, rendered from the chart with its live values
     (`SHARD_NODES="0,1,2,3,4;5@node-5,6@node-6"`).
   - The chart running the store's deploy guard (`DeployGuard`) in its hook Job before it POSTs
     (see below: "The guard on production").
   - A staging rehearsal of the whole sequence on a copy of real data.

1. **Choose the map.** Run `./gradlew :benchmark:bucketReport` (read-only; hours on production,
   off-peak) for events per bucket, and pick the map that gives each node its share. For 5 + 2,
   about `00-b5=0,b6-ff=1`. Prolific authors make hot buckets that no map can balance, so read
   the report's min/mean/max.

2. **Add the hosts.** Join two machines and scale the StatefulSet to seven pods, with the
   package still one cluster over nodes 0-4. The new pods run idle until the package names them.

3. **Overlap.** Set `VESPA_AUTHOR_BUCKETS` on the relay and the sync to the overlap map
   (`BucketMove.overlap.spec()`: `content` keeps every bucket, `content1` also receives
   `b6-ff`), restart both, then deploy the overlap package. From here every new write to a moving
   author lands in both clusters, and `content1` is pending: no read touches it. The guard lets
   this through (it only widens what clusters keep, and drops no node).

4. **Copy.** `BucketMove.copy()` copies the moving buckets inside Vespa (a visit with
   `destinationCluster`). For ~74/256 of ~945 M events (~273 M documents) at the 10,880 docs/s
   measured on one quiet 4-core host, about 7 hours. Production's 5 + 2 nodes copy in parallel,
   so likely less, but that is a projection, not a measurement.

5. **Diff until clean.** `diff(repair = true)` until it reports nothing. After a copy it is
   incremental: one pass reads the source's documents written since the copy began, and one
   streaming pass per cluster compares per-bucket digests, so only buckets that differ are read
   whole (author-shards.md, "Moving buckets"). The diff it replaced re-read the whole source once
   per bucket at production's bucket sizes: about 74 full scans for this map.

6. **Cut over.** Set the final map on the relay and the sync and restart them. Then run
   `finish { deploy the final package }`: it empties the moved buckets from `content` while the
   overlap still sends it every write, deploys the final package (which narrows `content`, so it
   needs `allowNarrowing`, given only after a clean diff), then removes what was written in
   between. `remaining()` must read zero.

## The guard on production

The store's deploy guard runs on every deploy the store makes. Production's deploys are made by
the chart's hook Job, which never calls the store, so today nothing checks a production package
before it is POSTed.

Two of the guard's checks matter there:

- **Buckets** (a cluster keeping fewer author buckets over data) matter only once production is
  sharded: one cluster keeps every bucket whatever the package says.
- **Node removal** (a content node dropped while it holds data and is not retired) matters
  **today**. Lowering `vespa.nodes` from 5 to 4 in the chart renders a package without node 4,
  and at redundancy 1 that deletes a fifth of the corpus. Nothing in the chart or in Vespa stops
  it.

So the guard is worth running in the hook as soon as a store release carries it, sharding or
not. The store ships it as a command in the engine jar, which the relay image already carries
(`DeployGuard`):

```
java -cp "/libs/*" com.nosfabrica.vespa.eventstore.engine.app.DeployGuard \
  --config-url http://<vespa-0>:19071 --query-url http://<vespa-0>:8080 /work/app
```

- It reads the assembled package (the directory the hook zips, or the zip), compares it with
  the package the config server has active, and counts the deployment's documents. All of it is
  read-only.
- Exit 0 means the package may deploy. Exit 2 means it would delete documents; the reason is on
  stderr: buckets a cluster would stop keeping, or nodes it would drop. Exit 1 means it could not
  judge (bad arguments, an unreadable package).
- The hook should POST only on 0.
- `--allow-narrowing` is for the bucket move's cut-over alone. It excuses buckets, never nodes.

The hook's deploy container is Alpine with no Java, so the step belongs in a container running
the relay image, which has a JRE and the jars: for example a second init container after the one
that copies the jars, writing its verdict where the deploy step checks it, or the deploy step
itself moved onto the relay image. That is a change to brainstorm-k8s, for its owners to make,
once a store release with `DeployGuard` is pinned in vespa-relay.
