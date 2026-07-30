# Scaling from the single-node quick start — operator guide

`VespaEventStore.open(url)` with the default `autoDeploy` is the quick-start
path: it deploys the bundled application package, whose `services.xml`
declares **one node, redundancy 1, and dev resource limits**. Scaling past
one machine means the OPERATOR owns the application package. This guide is
the workflow, what must stay verbatim, and what changes.

## The invariant: schemas travel with the library, topology travels with you

The store's YQL builder and the schema are released together so they can
never drift ("the schema ships with the code"). When you take over
deployment, that invariant must survive:

- **Keep verbatim** (from the bundled package):
  - `schemas/event.sd` and `schemas/reputation.sd` — every attribute,
    `match: cased`, `dictionary: hash`, the rank profiles. Never hand-edit;
    take them from the library version you run.
  - `search/query-profiles/default.xml` — `maxHits`/`maxOffset` at
    Int.MAX_VALUE, i.e. no engine-side hit ceiling. **Dropping this file does
    not remove a limit, it imposes one**: queries then silently cap at Vespa's
    default 400 hits. Lowering the numbers is a legitimate operator choice (a
    policy cap on expensive queries) — just know that a query asking for more
    is truncated without an error, so pair it with
    `VespaEventStore.open(maxHits = ...)` on the client side. For dumping the
    whole corpus, use a visit rather than a large search either way.

    A client-side `maxHits` bounds CALLER recall only — the store's own
    decision reads (dedup, NIP-09/62 guards, supersession) set
    `EventQuery.requireComplete` and ignore it, so capping recall can never
    corrupt a write. An engine-side cap has no such carve-out: it applies to
    every query, which is the reason to leave the query profile alone.
  - In `services.xml`: the **GC selection** on the event document
    (`selection="event.expires_at > now()"` + `garbage-collection="true"`) —
    NIP-40 expiry is enforced by the engine; dropping it silently disables
    that.
- **Change freely**: node lists, redundancy, groups, resource limits,
  `numthreadspersearch` (1 = concurrent-serving throughput; raise it for a
  latency-critical few-big-queries deployment), flush tuning.

## Workflow

1. **Extract the bundled package** for your library version — it is inside
   the `:vespa` jar as `vespa-app.zip` (`unzip vespa-x.y.z.jar vespa-app.zip`
   then unzip that), or programmatically via `VespaApp.zipBytes()`.
2. **Replace `services.xml`** with your topology —
   [`services-multinode-example.xml`](services-multinode-example.xml) is an
   annotated starting point (keep/change split marked inline).
3. **Deploy out of band** — `vespa deploy` or POST the zip to the config
   server's `prepareandactivate`, same as the library does.
4. **Open with `autoDeploy = false`** and name every container endpoint:

   ```kotlin
   VespaEventStore.open(
       url = "http://container-0:8080",
       autoDeploy = false,
       endpoints = listOf("http://container-0:8080", "http://container-1:8080", "http://container-2:8080"),
   )
   ```

   The feed client spreads its HTTP/2 connections across all endpoints
   (better than funnelling writes through one load-balancer address —
   connections-per-endpoint applies per endpoint) and reads round-robin. A
   single load-balancer URL also works.
5. **On every library upgrade**, re-extract the schemas from the new jar into
   your package and redeploy. Watch the deploy response: schema changes can
   carry *restart* or *re-index* actions (below).

## Schema migrations on a cluster that already holds data

The library's schema changes are validated on fresh deployments; an existing
corpus can need an extra step, which the deploy response spells out as
"restart actions" / "reindex actions":

- **`match: cased` + `dictionary: hash`** (the case-correctness fix): on a
  cluster that already holds events, this is a restart-class attribute
  change — deploy, then restart content nodes (rolling restart is fine). No
  re-feed is required: attribute values were always stored as fed; the
  restart rebuilds the dictionaries and switches matching to cased. Until
  the restart, tag matching stays case-insensitive (the pre-fix behavior).
- If a deploy is refused with a validation error naming an override id, add
  a scoped `validation-overrides.xml` to the package rather than forcing —
  and read what it protects first.

## Scaling the write path (constraints)

Adding content nodes scales the data, not the writer. The store's
replaceable/deletion/vanish enforcement needs writes for the SAME owner to
serialize through one store instance. One instance is correct at any cluster
size; when ingest outgrows it, shard the ingest by owner
(`hash(owner) % lanes`) across instances — every constraint is owner-scoped,
so lanes never need to coordinate. Full analysis:
[multi-node-consistency.md](multi-node-consistency.md).

Note the trust projection (`VespaReputationIndex`) writes through the single
`url` endpoint; reputation updates are low-volume, so this does not need the
endpoint fan-out.

## Sizing intuition (from the benchmark corpus)

The 30k-event corpus measured ~1.1 KB of stored fields per event; with index
and attribute overhead budget a few KB per event all-in. Tens of millions of
events fit a 16 GB content node comfortably; hundreds of millions want a
handful of nodes; query *throughput* (rather than corpus size) scales with
grouped distribution — each group holds a full copy and answers queries
alone. Re-run `:benchmark` (`BENCH_VESPA_URL`, `BENCH_THROUGHPUT=1`,
`BENCH_MIXED=1`) against your actual topology — every number in the README
came from a 4-core single node and is a floor, not a promise.
