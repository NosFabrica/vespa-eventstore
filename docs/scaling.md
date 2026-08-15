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
    Int.MAX_VALUE (no engine-side hit ceiling) and `grouping.globalMaxGroups`
    at `-1` (no group ceiling). **Dropping this file does not remove limits, it
    imposes them**: queries fall back to Vespa's defaults of 400 hits / 1000
    offset, and every aggregation the store issues starts failing.

    A query profile is the only place these can live at all — Vespa refuses to
    take any of the three from a request, failing it with `… must be specified
    in a query profile.` So the file is load-bearing, not a tuning file, and
    every field has to be carried over verbatim.

    `maxHits`/`maxOffset`: a query asking for more than the ceiling is
    REJECTED (`N hits requested, configured limit: 400`), not trimmed.
    Lowering them is a legitimate operator choice (a policy cap on expensive
    queries), but think hard first. The library holds no result cap of its own
    — a filter's `limit` is the only bound — so the number applies to EVERY
    query, including the store's own write decisions (dedup, NIP-09/62 guards,
    supersession). A guard read that errors out fails the write it was
    guarding. If you need to bound query cost, bound it where the filters are
    built, not here. For dumping the whole corpus, use a visit rather than a
    large search either way.

    `grouping.globalMaxGroups`: `count`, distinct-count, the kind histogram
    and the distinct-author sweep are all deliberately `max()`-less so they
    answer over the whole match set. While this ceiling is enabled, Vespa
    rejects a `max()`-less pipeline outright ("Cannot return unbounded number
    of groups"), so raising it back above `-1` breaks those paths rather than
    just capping them.

    `timeout` / `ranking.softtimeout.enable`: a filter with no `limit` is a
    request for the WHOLE match set, and it is allowed to take as long as that
    takes — the caller sets `limit` when they want a fast query. Vespa's
    default is the opposite: 500 ms, and because soft timeout is on it then
    stops matching and returns what it has, HTTP 200, `coverage.full: false`.
    So the deadline is at Vespa's maximum and soft timeout is off. Note
    `Query.setTimeout` rejects anything >= 1000000000 ms — do not "max" this
    field to 2147483647 the way `maxHits` is maxed, it fails every query.
    `VespaEventIndex` checks `coverage.full` on every search response and
    refuses a degraded one, so no other degradation source (node coverage,
    match-phase) can pass a partial answer off as a complete one either.
  - In `services.xml`: the **GC selection** on the event document
    (`selection="event.expires_at > now()"` + `garbage-collection="true"`) —
    NIP-40 expiry is enforced by the engine; dropping it silently disables
    that.
- **Change freely**: node lists, redundancy, groups, resource limits,
  `numthreadspersearch` (1 = concurrent-serving throughput; raise it for a
  latency-critical few-big-queries deployment), flush tuning.

## Workflow

1. **Extract the bundled package** for your library version — it is inside
   the `:engine` jar as `vespa-app.zip` (`unzip engine-x.y.z.jar vespa-app.zip`
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
- **`search_text_gram`** (the body's partial-word reach, 2026-08-15): a
  **reindex-class** change, and the one migration on this list that fails
  *silently* if you skip it. The column is derived by Vespa from `search_text`
  at index time, so a populated cluster keeps serving normally after the
  deploy — with the column empty for every existing document, no error and no
  400 anywhere. The whole back catalogue simply stays exact-token-only, which
  is the bug the column was added to fix. Verified on Vespa 8.

  The deploy response names it:

  ```
  "reindex": [{ "messages": ["Document type 'event': Non-document field
                'search_text_gram' added; this may be populated by reindexing"] }]
  ```

  Repair it either way:

  ```bash
  # (a) Vespa reindexing, on the CONFIG SERVER. Asynchronous — this returns
  #     immediately and the job goes `pending` until a maintainer dispatches
  #     it, so poll rather than treating the 200 as done.
  BASE=http://cfg:19071/application/v2/tenant/default/application/default/environment/prod/region/default/instance/default
  curl -X POST "$BASE/reindex?clusterId=content&documentType=event"
  curl "$BASE/reindexing"      # state: pending -> running -> successful

  # (b) A full re-feed. A plain put re-derives the column even with
  #     byte-identical content.
  ```

  **`reindexFullTextSearch()` does NOT repair this** — note the direction is
  the opposite of the near-tier columns. Those are *fed*, so only a re-feed
  can populate them; this one is *derived*, so the re-feed's drift check finds
  the search columns and near arrays unchanged, re-puts nothing, and reports
  success having done nothing.
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
