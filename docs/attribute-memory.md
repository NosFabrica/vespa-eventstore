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

**And that argument was checked against the engine rather than trusted.** Two
Vespa containers, one serving the pre-merge schema and one the merged schema,
the same crafted corpus fed to each (14 profile-name shapes — one-token,
multi-word, camelCase, separator-joined, diacritics, CJK — plus 5 article
titles), and the same 59-query battery run through the real store path against
both: prefixes at every length from 1 to full, 1- and 2-edit typos, camelCase
inner words, joined and separated variants, folded diacritics, CJK, and
multi-word phrases. **Every query returned a byte-identical id set.** 54 of the
59 return hits, so the comparison is not vacuous — the as-you-type ladder holds
from the 3-character prefix floor up (`ode`→3, `odel`→3, `odell`→2;
`vit`…`vitorpamplona`→14 throughout), typos recover (`odel1`, `odelll`,
`vitorpamplna`, `pamplna`), camelCase inner words and their joined forms recall
(`meme`, `treasury`, `memetreasury`), and each of `josé`, `中村`, `太郎`,
`中村太郎`, `比特币` matched on both sides.

The one thing this does NOT establish is within-band ORDER. `fieldMatch`'s
`queryCompleteness` divides by the terms in the query tree, and the merge
removed clauses from that tree, so `words_coverage()` can shift uniformly —
which changes the "words" rule's weight relative to exactness and perfect-match
INSIDE the top band, never band membership and never recall.
`RankRegressionIT` is green, so no calibrated case moved; that is evidence, not
a proof, and it is the honest limit of the claim.

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

**Migration — what a running cluster actually does.** Walked on a live Vespa:
old schema deployed, 20k events fed, then the new package deployed over it.

1. **The deploy is accepted with nothing to acknowledge.** HTTP 200, no
   `validation-overrides.xml` needed, and `configChangeActions` comes back
   `{"restart": [], "refeed": [], "reindex": []}`. **Vespa will not warn you.**
2. **The old attributes are deleted, immediately.** The four
   `name_parts`/`name_tokens`/`search_primary_parts`/`search_primary_tokens`
   directories under `.../0.ready/attribute/` are gone right after activation,
   and the document API stops returning those fields. Their memory is freed
   without a restart. (Whatever remains of the values inside the serialized
   document blob is inert and is rewritten on the doc's next put.)
3. **The new columns exist but are EMPTY for every existing document**, because
   the near tier is FED, not derived by an indexing expression. Measured on the
   migrated cluster: a `name_near` prefix query for an old profile returned
   **0 hits**, while the same profile's exact `name` query still returned 1.
   So the failure mode is not an outage — exact and gram recall are untouched —
   it is the near tier going **silently dark**: no prefix, no fuzzy,
   as-you-type recall collapses to the exact clause.
4. **`reindexFullTextSearch()` restores it**, and is the only thing that does.
   Run against the migrated cluster it re-put the drifted documents (it
   compares stored near arrays against a fresh derivation, so it finds them
   without being told); the same probe then read `name_near = [ranking893]`
   and the prefix query returned **1**.

So: deploy and re-feed are one operation, not two, and the ordinary Vespa
reindex will not substitute — nothing in the deploy response says so.

**The reverse order — new jar, old schema — used to be worse, and is the case
to know about.** `SchemaDeployer.deployIfAbsent` deliberately does *not*
redeploy over a live application (the operator owns `services.xml`; auto-
redeploying would replace a multi-node topology with the bundled single-node
one), so upgrading the library alone leaves the old schema serving. Reads
survived that — `SchemaFallbacks` catches the "field does not exist" 400 and
reruns without near clauses — but writes did not: measured against a real
Vespa, every insert of a searchable event died with

```
Status 400 … Field 'name_near' is not defined in document type 'event'
```

and took the whole batch with it, so **ingest stopped dead on a jar upgrade**.
The write path now carries the same net as the read path
(`VespaEventIndex.awaitPuts`): a refused document is re-fed without its near
columns, fail-open, landing it in exactly the state
`reindexFullTextSearch` already repairs. Re-verified on the same old-schema
cluster: 2,000 events, 1,875 stored including 51 kind-0 profiles, where the
run before the fix threw on the first profile.

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
- **Do not add a `search_text_near` attribute** to give the BODY prefix/fuzzy
  reach the way `name_near` and `search_primary_near` do for names and titles.
  This was proposed 2026-08-15 to fix a real recall bug (a kind 1 reading
  "Testing post to #vegans group!" was found by "testing" and not by "testin")
  and rejected on this model. Every other near column is cheap because it is
  short and often empty — `name_near` sits at `v ≈ 0.07` elements/doc,
  `search_primary_near` at `v ≈ 1.08`, ~4 GiB each. A body column is filled on
  nearly every document at `v ≈ 30`, and both the multivalue mapping and the
  posting lists scale as `D · v`:

  | cap | modelled total | of which unpageable |
  |---|---:|---:|
  | `NearText.MAX_ELEMENTS` (48/96) | **73.8 GiB** | 48.1 GiB |
  | tightened to ~16 | 30.2 GiB | 19.3 GiB |
  | tightened to ~8 | 20.5 GiB | 12.9 GiB |

  The cheapest version still adds more than a third of the current 55.8 GiB
  attribute total to a cluster already OOM-killing, and *every* version is
  capped — so a blog post would be reachable only by its first few dozen tokens,
  which is the failure the bug report was about. The answer is
  `search_text_gram`: a gram-3 **index** field, matched as a trigram phrase. An
  inverted index scales with vocabulary rather than corpus × length, costs
  nothing in the attribute budget, has no per-document cap, and — verified on
  Vespa 8 — matches exactly the documents that contain the substring, where the
  AND-of-trigrams net the other gram fields use invents hits at up to 4.0% on
  long bodies. It buys substring reach instead of anchored-prefix reach and no
  typo forgiveness in bodies; that is the trade, and it is the right one.

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

## 7. The growth reserve — what `allocation.growfactor` can and cannot reach

Study for [#206](https://github.com/NosFabrica/vespa-eventstore/issues/206):
staging attributes allocated ~20% ahead of what they use (`id` 24.81 GiB
allocated against 20.33 GiB used), counted by the feed block, given back by a
restart and re-taken by the next growth step. The proposal was to lower
proton's `documentdb[].allocation.growfactor` / `multivaluegrowfactor` (both
default 0.2) through a `<config name="vespa.config.search.core.proton">`
override in `services.xml`. Measured first, as the issue asked, on Vespa
8.781.11 with `AttributeGrowthIT` (`benchmark/`, below). **The override cannot
be shipped: it never reaches proton. And even if it did, it would govern only
about two fifths of the reserve.**

### 7.1 A `services.xml` override of `allocation` is a silent no-op

`documentdb` is an array in `proton.def`, and the config model **appends** a
user override of an array to the entries it generates; it does not merge into
them. The config server then serves the search node two `event` entries: the
model's own entry first, still at 0.2, and the override after it. Proton picks a
document type's entry by the FIRST `inputdoctypename` match
(`find_document_db_config_entry` in `documentdbconfigmanager.cpp`), so it reads
the default:

```
served documentdb entries (stock):     reputation 0.2, event 0.2
served documentdb entries (override):  reputation 0.2, event 0.2, event 0.05
```

Placing the `<config>` on the content `<node>` instead of the cluster appends it
the same way. Nothing in `services.xml` exposes `allocation` either.
`searchnode > resizing` only covers `initialnumdocs` / `amortize-count`, and is
deprecated and ignored. The deploy returns 200 with no config change actions
either way, so **Vespa will not tell you**.

Measured, not inferred. At 3,000,000 documents the two packages allocate the
same bytes, attribute by attribute, to 0.1 MiB, including every single-value
document vector, which is exactly what `growfactor` sizes. The six single-value
attributes each hold 14.0 MiB allocated against 11.4 used, 1.23× (a 0.05 factor
would leave at most ~1.05×):

| allocated MiB @ 3.0M docs | stock | override 0.05 |
|---|---:|---:|
| `id` | 364.3 | 364.3 |
| `tag_index` | 304.4 | 304.3 |
| document meta store | 130.8 | 130.8 |
| `created_at` / `kind` / `expires_at` | 47.3 / 30.8 / 27.7 | 47.3 / 30.8 / 27.7 |
| all attributes + meta store | 1,154.6 | 1,154.6 |

Feed rate was the same within noise: 5.3–6.6k docs/s per segment in both runs.

**The tempting fix is worse.** Leave out `<configid>` from the override item and
the config server cannot build the proton config at all. It serves the search
node an EMPTY one and logs only at INFO (`Unable to resolve instance for builder
'ProtonConfig$Builder', returning empty config … must be initialized:
[configid]`). Proton then logs `No document databases in config, trying to
re-fetch bootstrap config` and keeps the config it had. Observed over a live
document db: it keeps serving, but takes no further config change (a rank
profile, a tuning) until the package is fixed. Restating every entry with its
`configid` does not help either: it is still appended.

### 7.2 Where the reserve actually is, and which rule grows it

Proton's state API splits each attribute's memory by component
(`/state/v1/custom/component/documentdb/event/subdb/ready/attribute/<name>`).
The rule that grows each component is in Vespa's source (master, read
2026-11-11):

| component | grows by | settable? |
|---|---|---|
| per-document vector (lid → value / enum ref), meta store lid table | `TypeStableVector` × (1 + `growfactor`) | `allocation.growfactor`, unreachable (§7.1) |
| multi-value array store (`tag_index`, the near columns) | array-store buffers × `multivaluegrowfactor` | `allocation.multivaluegrowfactor`, unreachable (§7.1) |
| enum-store values (every distinct `id`, `pubkey`, tag value, …) | `ALLOC_GROW_FACTOR = 0.2`, hard-coded (`unique_store_allocator.hpp`, `unique_store_string_allocator.cpp`) | no |
| B-tree dictionary and posting lists | `DEFAULT_ALLOC_GROW_FACTOR = 0.2`, hard-coded (`buffer_type.cpp`) | no |
| **hash** dictionary (`id`, `pubkey`, `owner`, `tag_index`: `dictionary { hash }`) | a full shard **doubles** (`size*2+2`, 3 shards; `sharded_hash_map.cpp`) | no |

Measured at 3,000,000 documents (stock, settled; the attribute statistics lag
their components by seconds, so the IT pauses the feed until two reads agree).
At that checkpoint the reserve was 146.9 MiB, 12.7% of allocation. Across the
ten checkpoints it ranged from 12.7% to 23.7%, a sawtooth set by where each
structure sits in its own growth cycle:

| rule | reserve MiB | share |
|---|---:|---:|
| hard-coded: dictionaries (of which `id`'s hash dictionary 43.7: 94.0 allocated, 50.3 used) | 52.0 | 35% |
| hard-coded: enum-store values | 25.0 | 17% |
| hard-coded: posting lists (+ change vectors 0.7) | 13.3 | 9% |
| `multivaluegrowfactor`: multi-value arrays | 24.3 | 17% |
| `growfactor`: six single-value document vectors 15.9, `author_ref` 10.9, meta store 5.7 | 32.5 | 22% |

The last row is an upper bound for `growfactor`. `author_ref` and the meta store
also carry B-tree structures that the state API does not split out.

So **~61% of the reserve grows by rules no configuration reaches.** Had the
override worked, 0.05 would have cut the other ~39% by at most three quarters:
~29% of the reserve, ~4% of attribute memory at this checkpoint. The largest
single item is the one #206 pointed at, `id`. But its reserve is mostly its hash
dictionary, which doubles, not the 20% steps the issue assumed.

### 7.3 What this does not establish

- **Shares at staging scale.** 3M documents is ~1.5% of one staging node
  (~200M). A buffer's 20% step is scale-free, but a hash shard's doubling is
  not a steady fraction, and the fixed minimum buffer sizes still weigh at 3M.
  The same read answers this on staging directly, read-only, one attribute at a
  time (the state port comes from `vespa-model-inspect service searchnode`,
  `STATE` in its listing):
  ```bash
  curl -s localhost:19113/state/v1/custom/component/documentdb/event/subdb/ready/attribute/id \
    | jq '{total: .status.memoryUsage, values: .enumStore.valuesMemoryUsage,
           dictionary: .enumStore.dictionaryMemoryUsage, postings: .posting_store.memory_usage.total}'
  ```
  If `id`'s dictionary carries GiBs of slack there, as its 46% here suggests,
  that is the reserve to name in an upstream Vespa request.
- **How much of the reserve is resident.** At 3M documents attributes are
  ~40% of proton's `RssAnon`. `RssAnon` swung by ±1 GiB between checkpoints as
  the memory index filled and flushed (809 → 1,341 → 859 MiB), which buries any
  growth-step transient. Peak `RssAnon` was 3.06 GiB (stock) and 3.17 GiB
  (override, same config in effect). The transient's mechanism is in the source:
  a document vector copies into the new array and holds the old one until
  readers release it (`TypeStableVectorBase::expand`), and a hash shard holds
  its old map the same way. Its size at staging scale is not measured.
- **Online shrink.** The source has one path: lid-space compaction calls
  `TypeStableVectorBase::shrink`, which still keeps `growfactor` headroom over
  the new size. Datastore buffers give space back only by compaction of dead
  entries (`max_dead_bytes_ratio`). Neither returns an unfilled reserve, which
  matches the restart-only recovery #206 observed.

### 7.4 What follows

- **Do not add `allocation` to `services.xml`**, bundled or the brainstorm-k8s
  override. Appended, it does nothing. Without `configid`, it freezes proton's
  config. The bundled `services.xml` says so next to its proton `<config>`.
- **The lever is upstream.** Vespa would have to expose `allocation` through
  the content cluster's tuning (the model sets every other `documentdb[]`
  field), or merge array overrides by key. Even then, most of the reserve
  measured here is hard-coded growth that no setting reaches.
- **#206's own third point stands, and is now the whole answer.** The reserve
  buys margin, not capacity; the real fixes are more memory per node or a
  fifth content node (§4.6). Rolling restarts still return it, but only until
  the next growth cycle.

```bash
# the measurement (Docker; ~20 min for both variants at 3M docs on 4 cores)
./gradlew :benchmark:test -Pintegration -DitGrowDocs=3000000 \
    -DitGrowVariants=stock,0.05 --tests '*AttributeGrowthIT*'
grep -ho 'tests="[0-9]*" skipped="[0-9]*"' benchmark/build/test-results/test/*AttributeGrowthIT*.xml
```
