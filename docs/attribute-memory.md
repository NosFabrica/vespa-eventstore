# Attribute memory — what moving fields out of RAM actually costs

Study for [#69](https://github.com/NosFabrica/vespa-eventstore/issues/69):
proton holding 75 GiB resident for 176.7M documents (~456 B/doc), six OOM
kills in 28 hours, every attribute in memory and none `paged`.

The issue's own conclusion is the right one — *"without per-field measurements,
any edit is a guess"* — so this document is in two halves. First **where the
bytes are and which lever can move which byte**, derived from Vespa's sizing
formulas and verified against Vespa's source, because that part does not need
a live cluster. Second **the per-field verdicts against this store's actual
query shapes**, which is the part the issue is really asking for: not "can this
field be paged" but "what breaks in the read and write paths when it is".

`benchmark/attribute_memory.py` is the measurement side: it prints the measured
per-field table from a live deployment, and the modelled component breakdown
that measurement cannot give (Vespa reports one number per attribute; the lever
you pick depends on which component inside it you are trying to move).

```bash
# where the RAM actually went
python3 benchmark/attribute_memory.py --metrics http://vespa:19092/metrics/v2/values?consumer=Vespa

# what each lever could free
python3 benchmark/attribute_memory.py --model --docs 176700000
```

## 1. The mechanism

A Vespa attribute is not one structure. It is up to five, and `paged` moves
exactly three of them:

| component | what it holds | per-field cost | `paged` moves it? |
|---|---|---|---|
| document vector | local doc id → value (or enum index / multivalue index) | `D · 4 · ROF` for anything enum-backed | **yes** |
| multivalue mapping | doc → its array elements | `D · V · 4 · ROF` | **yes** |
| enum store *values* | each distinct value, once | `U · (FW + VW + 4) · ROF` | **yes** |
| enum store *dictionary* | the lookup structure over those values | `U · (EIW + PIW) · ROF` | **no** |
| posting lists | value → doc ids, built by `fast-search` | `D · V · PW · ROF` | **no** |

Vespa's docs state the resident half as *"for attribute fields using
`fast-search`, the memory needed for dictionary and index structures are never
paged out to disk"*. The split is visible in the source: `AttributeVector`
builds an mmap-file allocator when `paged` is set
(`make_memory_allocator`/`allow_paged`), and that allocator is threaded into
the enum store's value store (`EnumStoreT`'s `_store`), the multivalue mapping
(`MultiValueAttribute`'s array store) and the single-value document vector
(`get_initial_alloc`) — and **into nothing else**. The enum store's dictionary
(`make_enum_store_dictionary`) and `PostingListAttribute`'s posting store are
constructed without it. `paged` is refused outright only for `predicate` and
for a non-dense `tensor` that also sets `fast-search`; every field in
`event.sd` is legal to page. Legal is not the same as free.

Two consequences that decide everything below:

- **A dictionary lookup costs one page touch per query TERM. An attribute-vector
  read costs one per HIT.** Paging a field that only ever appears in a `where`
  clause is bounded by the query's width (500 ids → ~500 touches). Paging a
  field that is *sorted on*, *grouped by*, *read by a rank expression*, or
  *returned in a summary* is bounded by the match set — Vespa's own warning:
  *"Using a paged attribute in first-phase ranking can result in extremely high
  query latency… the number of disk accesses will, in the worst case, be equal
  to the number of hits the query produces."*
- **Prefix and fuzzy matching read values, not just the dictionary.** A btree
  dictionary is ordered by value, so walking it to find `ode*` or everything
  within edit distance 2 compares the *strings* — which live in the paged half.
  That is the second warning: *"A similar problem can occur if running a query
  that searches a paged attribute."*

## 2. Where the 75 GiB is

Modelled at D = 176.7M with the corpus assumptions in
`attribute_memory.py` (`DEFAULT_PARAMS` — each is labelled and each is
checkable against the store; they are the only guesses here). This is the
schema as it stands, i.e. **after** the near-column merge of §4.1; the state
#69 reports ran the pre-merge schema and modelled at 59.5 GiB.

| field | decl | total GiB | docvec | mvmap | enum values | dict | postings |
|---|---|---:|---:|---:|---:|---:|---:|
| `tag_index` | array\<string\> | 17.69 | 0.79 | 2.37 | 8.72 | 1.07 | 4.74 |
| `id` | string | 16.79 | 0.79 | — | 13.63 | 1.58 | 0.79 |
| `search_secondary_tokens` | array\<string\> | 4.00 | 0.79 | 0.95 | 0.23 | 0.13 | 1.90 |
| `search_primary_near` | array\<string\> | 3.96 | 0.79 | 0.85 | 0.39 | 0.22 | 1.71 |
| `created_at` | long | 2.92 | 0.79 | — | 0.80 | 0.54 | 0.79 |
| `pubkey` | string | 2.01 | 0.79 | — | 0.39 | 0.04 | 0.79 |
| `owner` | string | 2.01 | 0.79 | — | 0.39 | 0.04 | 0.79 |
| `expires_at` | long | 1.58 | 0.79 | — | 0.00 | 0.00 | 0.79 |
| `kind` | int | 1.58 | 0.79 | — | 0.00 | 0.00 | 0.79 |
| `affil_tokens` | array\<string\> | 1.23 | 0.79 | 0.09 | 0.10 | 0.05 | 0.19 |
| `name_near` | array\<string\> | 1.08 | 0.79 | 0.06 | 0.08 | 0.04 | 0.11 |
| `author_ref` | reference | 0.90 | 0.79 | — | 0.09 | 0.02 | — |
| **attributes** | | **55.8** | | | | | |
| document meta store | implicit | 4.9–8.6 | | | | | |

The pre-merge model — 59.5 GiB of attributes, so ≈ 64–68 GiB all-in — against
75 GiB measured; the rest is the memory index for the ten BM25 text fields, the
`removed` sub-database's own meta store, the reverse mapping `author_ref` keeps
(parent → child local ids, which Vespa publishes no formula for, so that row is
a floor), and proton's fixed overhead. Close enough that the *ranking* is
trustworthy, which is all a model is for. Four things fall out of it:

**Two fields are 60% of it.** `id` and `tag_index` together model at 34.5 GiB.
Both for the same reason: an enum store holding 64-character hex, once per
distinct value, and `id` has no repeats by construction — its 13.6 GiB enum
store is 176.7M copies of a string that is *already the document id*.

**Every attribute costs 4.8 B/doc before it holds anything.** Twelve document
vectors × 0.79 GiB = 9.5 GiB of pure per-document slots (fourteen and 11 GiB
before the merge). The near-tier arrays pay theirs on all 176.7M documents even
though only kind-0s and titled kinds fill them — this is Vespa's *"`fast-search`
causes a memory increase even for empty fields"*, and it is why "the field is
usually empty" is not an argument for keeping a column.

**17.1 GiB of the total cannot be paged at all** (dictionaries + posting lists).
Paging *every* attribute in the schema would free ~39 GiB and leave ~17 GiB —
which is the ceiling on the whole `paged` strategy, before counting what it
costs.

**The page cache is already starved, and that is a present-tense read cost.**
The issue reports anon 76.09 GiB against file 3.42 GiB in an 80 GiB cgroup.
The document store is 334 GiB and every REQ that returns events fills its
summary from it (a document's fields are one compressed blob — the
`SUMMARY_FIELDS` trim cuts transfer and parse, never the disk read). With
~3 GiB of cache for a 334 GiB store, effectively every summary fill is a cold
read. Freeing attribute memory buys page cache for the document store and the
disk index; that second-order win is likely larger than any latency the same
change costs.

**Restart is the peak, not steady state.** Loading a multivalue attribute
allocates an extra `D · V · 12` transiently (local doc id + enum value + weight
per value). At the modelled V that is ~5.9 GiB for `tag_index` and ~4.9 GiB
across the near-tier arrays — ~11 GiB of transient allocation on top of a
steady state already at 94% of the limit. A cluster that OOM-kills at steady
state will OOM *harder* during the restart that follows, which is the shape of
"six kills in 28 hours".

### How far the model can be trusted

The §4.1 A/B put the model next to a real engine for the first time — same
corpus, measured per-field bytes. What that comparison says:

- **The ranking is right, which is what the model is for.** Measured at 186,867
  documents, `id` is 25.4% of attribute memory and `tag_index` 24.1% — together
  half of it, the same two fields on top, in the same order, for the same
  reason. Every decision in this document rests on that ordering.
- **The absolute per-field bytes run 1.1–2× the formulas at small scale.**
  `allocated_bytes` includes dead and on-hold bytes and Vespa's buffer slack
  beyond the 6/5 resize factor, and those fixed costs amortize as the corpus
  grows — measured 478 B/doc of attributes at 187k documents against the 456
  B/doc *total* #69 reports at 176.7M. So the formulas are a floor, and the
  gap narrows with scale.
- **Where the model was wrong it was optimistic by ~20%** (3.78 GiB predicted
  vs 3.09 GiB measured for the merge). Treat every saving on this page as an
  upper bound until it has been measured the same way.

That is the standing method: model to rank the candidates, measure to size the
one you picked.

## 3. Per-field verdicts

What each field is touched by, in this store's code, and what paging or
un-indexing it would do. The column that matters is "read per hit" — that is
what turns a page fault into a per-result disk seek.

| field | matched by | read per hit by | verdict |
|---|---|---|---|
| `created_at` | every filter's `since`/`until` range | `order by created_at desc` (every plain recall), `recency`/`recency_gated` **match-phase**, `first-phase: attribute(created_at)`, the `idtime`/`idtimetag` summaries | **never page.** It is read in first-phase ranking of every match and again while sorting — the textbook worst case |
| `id` | `existingIds` (500-id `in`, the hottest write-path query), `getByIds` | the `dedup` summary class, `idtime`, `idtimetag` | **never page.** The dedup preload is 500 attribute reads per query at ~12 ms today; a page fault per hit would undo the 2.3× that summary-free check bought |
| `tag_index` | every `#e`/`#p`/`#t` filter | the `idtimetag` summary (addressable-corpus walks) | **do not page.** Matching alone would survive (postings stay resident), but the `d`-tag walk reads it per hit, and 6.9 GiB of its 17.7 GiB is resident anyway |
| `pubkey` | author filters | `buildDistinctAuthors` grouping (the orphan sweep reads it for **every** match) | **do not page.** 2 GiB is not worth putting a fault in a full-corpus grouping |
| `kind` | `kind in (…)` — often the only selective clause | `buildKindHistogram` grouping | leave alone; 1.6 GiB, and dropping `fast-search` turns the commonest REQ into a scan |
| `owner` | NIP-09 / NIP-62 guard reads | — | leave alone; 2 GiB, load-bearing for deletion semantics |
| `expires_at` | ANDed into **every** query (`expires_at > now`) plus the sweep | — | **drop `fast-search`** — see below |
| `author_ref` | — | imported tensors, read per ranked hit | leave alone; already the cheapest column |
| `name_near`, `search_primary_near` | prefix + fuzzy, search queries only | — | the four columns these replaced are §4.1, landed. Do not page them: fuzzy walks the dictionary comparing the paged strings |
| `search_secondary_tokens`, `affil_tokens` | prefix only, search queries only | — | the only defensible `paged` candidates in the schema, and worth ~2.5 GiB (see §4.4) |

## 4. Ranked candidates

Savings are modelled, not measured. Every one of them needs the number from
`attribute_memory.py --metrics` first — the ranking is stable under the
assumptions, the absolute numbers are not.

### 4.1 Merge the four near-tier columns into two — 3.78 GiB — **LANDED**

`name_parts` and `name_tokens` were **never distinguished**, anywhere:
`FuzzyWordGroup` emitted an identical prefix clause and an identical fuzzy
clause against each (`NEAR_FIELDS`), and `event.sd` only ever asked
`matchCount(name_parts) > 0 || matchCount(name_tokens) > 0`
(`loose_name_match`). Same for `search_primary_parts`/`search_primary_tokens`
(`tier_loose_match`). Two columns holding a union that was OR'd back together at
both ends. They are now `name_near` and `search_primary_near`
(`NearText.mergeNear`).

**Nothing is lost, by construction rather than by measurement.** Each source
list is capped at `MAX_ELEMENTS` (48) *before* the merge, so their union cannot
exceed `MAX_MERGED_ELEMENTS` (96) and the cap never bites — the merged column is
exactly `(parts + tokens).distinct()`. `NearTextTest` pins that equality over
every derivation code path plus the shapes that stress both caps (120-word
field, CJK, camelCase, diacritics), and pins that the surviving column still
carries both granularities' reach. The per-document dictionary bound is
unchanged too: 48+48 across two columns, 96 across one.

Nothing can tell the difference at query time either: the match set is the union
either way, and both rank functions only ever tested `> 0`, so band membership is
bit-identical. Neither field is read per hit by a summary, sort, grouping or
rank expression, so there is no second path to check.

**What it buys — MEASURED on a real engine.** Two Vespa containers, the
identical 200k-event `NostrCorpus` fed into each (186,867 live documents after
supersession and deletion, both runs), per-field attribute memory read off the
metrics API:

| | before | after | saved |
|---|---:|---:|---:|
| `name_parts` + `name_tokens` | 18.4 B/doc | `name_near` 8.8 | |
| `search_primary_parts` + `search_primary_tokens` | 19.4 B/doc | `search_primary_near` 10.3 | |
| **the near columns** | **37.9 B/doc** | **19.1 B/doc** | **−49.6%** |
| all attributes | 478.4 B/doc | 458.2 B/doc | −4.2% |

**18.8 B/doc, which is 3.09 GiB at the issue's 176.7M documents.** The model
said 3.78 GiB, so it runs ~20% optimistic here — its `v` assumption for the
merged columns credited slightly more overlap than the real derivation delivers.
Half the saving (9.6 B/doc) is the two document vectors, which is exact and
scale-invariant; the rest is the granularities' overlap, and that part is
corpus-shaped.

That overlap is separately measured by `NearMergeSizingTest`, which derives both
columns through the real extraction path and counts elements:

| corpus | pair elements | merged | saved |
|---|---:|---:|---:|
| weighted mix of real name shapes | 69 | 47 | **31.9%** |
| …of which `parts == tokens` (the one-word profile name) | 16 | 8 | 50.0% |
| …of which `parts != tokens` | 53 | 39 | 26.4% |
| `NostrCorpus` kind-0 names (all one token) | 1766 | 883 | 50.0% |
| `NostrCorpus` article titles (`search_primary`) | 4491 | 2578 | 42.6% |

It also **halves the prefix and fuzzy clause count per query word** — four near
clauses become two, and fuzzy is the most expensive matcher in the query. That
is a search-latency win the model does not price.

**Verified.** Both gates are green against this change:

- `./gradlew build` — `spotlessCheck` plus every unit test in all three
  modules, including the wire-shape pins in `EventYqlTest`, the
  schema/query-consistency guard in `VespaAppTest`, and the near-field
  compatibility demotion (`SchemaFallbacks`, `MockVespaEngine`).
- `./gradlew :benchmark:test -Pintegration` against real Vespa containers —
  **8/8, no skips**: `VespaParityIT` (127/127 filter checks agree with SQLite),
  `SearchPrefixLadderIT` (the as-you-type ladder, the property this merge could
  most plausibly have broken), `RankRegressionIT` (the rank-band cases),
  `SearchExactTextIT`, `ObserverGateIT`, `OrphanSweepIT`, `AccessLogIT`. Only a
  real engine executes the schema, so this is also the proof that the merged
  `.sd` deploys at all.

**Migration:** this is an attribute add + remove and a feed-side change, so an
existing corpus needs a re-feed, not just a reindex —
`NostrSemanticsStore.reindexFullTextSearch` is that re-feed and detects the
drift automatically (the stored near arrays no longer equal a fresh derivation).
A new client against an old serving schema degrades rather than fails:
`SchemaFallbacks` catches the "field does not exist" 400 and reruns without near
clauses.

### 4.2 Stop storing 64-hex `id` as a string attribute — ~9.7 GiB

28% of attribute memory, 13.6 GiB of it an enum store holding one unique
64-character string per document — a string Vespa already holds as the document
id, and which the summary already carries.

Replace with two numeric attributes and keep `id` as `indexing: summary` only:

```
field id_hi type long { indexing: attribute | summary   attribute: fast-search }
field id_lo type long { indexing: attribute | summary }          # no fast-search
```

`id_hi`/`id_lo` are the first two 64-bit words of the id. Matching is
`id_hi in (…)`; `id_lo` rides the attribute-only `dedup` summary so the client
confirms the full 128 bits and set membership stays **exact** (a false positive
needs a 128-bit collision). Modelled: 5.5 GiB + 1.6 GiB against 16.8 GiB.

The `id_hi`-only variant saves another 1.6 GiB and is *not* exact: a 64-bit
prefix collision between a stored document and a newly offered event would
silently drop the write. At this corpus that is ~5e-9 per 500-id chunk — small,
but "silently drops an event" is the wrong kind of small, and this store pays
for exactness everywhere else.

Cost: `EventYql.hexIn`, `buildExistence`, `EventDoc`, `MockYql`,
`InMemoryEventIndex` and the parity battery all move together. Not a schema
edit — a change to the id contract.

### 4.3 Drop `fast-search` from `expires_at` — ~0.8 GiB, probably a latency win

`expires_at` is ANDed into every single query as `expires_at > now`, a range
that matches essentially every document (`Long.MAX_VALUE` = never expires). A
`fast-search` range over almost every value is the worst possible use of a
posting list; as a plain attribute it becomes a per-candidate filter evaluated
only for documents the selective clauses already produced. The posting list
(0.79 GiB) is pure overhead.

Cost: the expiry sweep (`expiresBefore`, a bare range with no selective
companion) becomes a scan. It is periodic maintenance, not a serving path — but
measure it, and measure a match-all count, before shipping.

### 4.4 `paged` on `search_secondary_tokens` + `affil_tokens` — ~2.5 GiB, measure hard

The only two near-tier columns that are **prefix-only, never fuzzy**
(`PREFIX_ONLY_FIELDS`), touched by search queries only, and never read per hit
by a summary, sort, grouping or rank expression. That makes them the schema's
only fields whose paged half is touched at query-term granularity rather than
per hit.

Even so: a prefix walk over a btree dictionary reads paged strings, `paged`
doubles these fields' attribute disk, and — Vespa's warning — the memory metrics
stop meaning what they say once anything is paged. Gate on
`content.proton.documentdb.matching.*` latency and the search p99 before and
after, on a corpus large enough that the file cannot sit in page cache.

### 4.5 Hash `tag_index` to `array<long>` — ~7 GiB, costs exactness

8.7 GiB of `tag_index` is an enum store of `"<letter>:<64-hex>"` strings. Quartz's
SQLite store — the parity target — indexes a `tag_hash`, not the value, so there
is precedent. Modelled at 10.6 GiB against 17.7.

It is listed last of the real levers because it trades exactness: a 64-bit
collision across ~1.2e8 distinct tag values has ~4e-4 probability corpus-wide,
and a collision makes a tag filter return extra events. Recall can be made
exact again by post-filtering hits against the `tags` JSON already in the
summary; `count`, the kind histogram and the distinct-author sweep cannot.
It also breaks the `idtimetag` `d`-tag walk (a hash does not walk back to a `d`
value), which would need a separate small `d_tag` attribute.

### 4.6 Structural — the levers that actually change the slope

- **Split the search columns into their own document type.** The four near-tier
  arrays, ten BM25 index fields and six gram views exist for the minority of
  kinds that are searchable, and are paid for on every document. The benchmark
  README already names this ("a real option at scale, at the cost of dual
  writes"); it also halves the document-store blob for notes, which is where the
  page-cache starvation above is being paid.
- **Retention.** Every number on this page is linear in D, and a
  `created_at > now() - N` garbage-collection selection in `services.xml` is the
  only lever that reduces D. It is one line, next to the NIP-40 selection that
  is already there.
- **A second content node.** Halves everything per node — except the
  `reputation` schema, which is `global` and therefore replicated to every node
  in full. That is a per-node constant that sharding does not reduce, and it is
  worth measuring before assuming a node split is proportional.

## 5. What not to do

- **Do not page `id`, `created_at`, `tag_index` or `pubkey`.** Each is read once
  per *hit* (summary fill, sort, match-phase, first-phase, grouping), which is
  precisely the access pattern Vespa documents as pathological, and between them
  the un-pageable half stays resident anyway.
- **Do not page anything that is fuzzy-matched** (`name_near`,
  `search_primary_near`). The fuzzy walk compares the strings that paging moved
  to disk.
- **Do not drop `fast-search` from `id`, `pubkey`, `kind` or `tag_index`.** Each
  becomes a linear scan of 176.7M documents on the store's commonest query
  shapes.
- **Do not enable `paged` broadly to "see if it helps".** Removing it later
  requires loading the attribute fully into memory with no page-out option —
  Vespa's *"might cause hard out-of-memory situations"* — so the rollback is
  more dangerous than the change.
- **Do not trust memory metrics after enabling `paged`.** They report attribute
  content size, not residency; the node will read as ~100% utilized by design.

## 6. Getting the measurements

```bash
# 1. per-field, ranked — the table this whole document is a model of
python3 benchmark/attribute_memory.py \
    --metrics 'http://vespa:19092/metrics/v2/values?consumer=Vespa'

# 2. the component breakdown inside a field (undocumented proton debug API,
#    per attribute — enum store, multivalue mapping, posting store)
curl -s http://vespa:19110/state/v1/custom/component/documentdb/event/subdb/ready/attribute/tag_index

# 3. the corpus parameters the model guesses at, from the store itself:
#    U for tag_index  -> buildDistinctCount(tag_index)
#    U for pubkey     -> buildDistinctAuthors over a match-all
#    V                -> total tag values / documents, from a visit sample
```

The `field` dimension is not in the default metrics consumer set — ask for
`?consumer=Vespa`, or read the content node's own `/state/v1/metrics`. Check
the `removed` sub-database as well as `ready`: a large `removed` meta store is
its own finding (lid-space bloat), not part of the steady state you are trying
to shrink.

One trap, found by running the tool against a live node rather than a saved
sample: `/metrics/v2/values` publishes these gauges **only** as `.average` —
there is no `.last` there at all, though `/state/v1/metrics` carries the whole
set. A reader that knows only `.last` therefore finds nothing on the very
endpoint #69 quotes, and reports it as "no per-field metrics" rather than as a
parsing gap. `attribute_memory.py` now takes the best available aggregation per
metric.
