# The embedding experiment: does dense retrieval improve search?

Status: **SHIPPED as an intent-gated perfect-match rung (§10), with no embedding in it. The embedding itself did not earn its place (§7, §8.2). 958 model grades are owed a 20% human audit (§9.3).**
package is built from a COPY of `engine/app`, and the shipped schema is unchanged.

## 1. The question

Search today matches words only. That means BM25 and fieldMatch, the prefix and fuzzy
near tier, the phrase band and joined spellings, all multiplied by trust. It is strong on
names and weak where the query and the document use different words. The 54-case
evaluation (`search-evaluation.md` §9) shows this in the **topical** stratum. A note about
coffee that never says "coffee" cannot be recalled. A note that does say it ranks on the
body rung (550) beside every passing mention, and the only signal for whether a document
is ABOUT the query is the tag-plus-mention stack.

The hypothesis is that a multilingual sentence embedding adds the two things missing:
**recall** of on-topic documents that lack the word, and an **aboutness** signal inside the
bands. It should not move navigational or prefix queries.

The bar was set before running anything: **topical nDCG@10 +0.05 or more** on human grades,
with no pinned navigational or prefix case LOST. Under that, the RAM cost on production is not
worth paying (§6).

## 2. What is built

| piece | file | what it does |
|---|---|---|
| experiment package | `benchmark/embedding/build_app.py` | adds an `e5` `hugging-face-embedder` (intfloat/multilingual-e5-small, 384-d, `query: `/`passage: ` prefixes, mean pooling, normalized), a synthetic `embedding` field (every naming and text field, embedded at feed time, bfloat16 HNSW, angular), and four knobs on the `search` profile |
| background slice | `benchmark/embedding/capture_background.py` | recent kind 1 and 30023 events by the lens's most-trusted authors, the same for every topic, never chosen by a topic's words |
| configs | `RankAb.SEMANTIC_CONFIGS` | the sweep below. Run only when named, because they fail on a stock cluster |
| hybrid recall | `RankAb.search` (`_nn`) | a second, nearestNeighbor-only request under the same filters, lens and profile, merged with the lexical page by score |

### 2.1 The knobs (all inert at their defaults)

```
first phase   max(floored_text_score(), w_sem_tier * sem_norm()) * wot_mult() * recency_mult()
second phase  (...as shipped...) * phrase_mult() * (1 + w_sem_mult * sem_norm())
sem_norm      clamp((cosine(embedding, q_emb) - sem_floor) / (sem_ceil - sem_floor), 0, 1)
```

- `w_sem_tier` (default 0) is a **semantic rung**. A dense-only hit has no text score, so
  without a rung the drop limit deletes it. 0 leaves `max(x, 0) = x` on every document.
- `w_sem_mult` (default 0) is **aboutness inside a band**. 0 multiplies by 1.
- `sem_floor` 0.84 and `sem_ceil` 0.90, measured on this corpus. Across ~11k notes and
  articles, the best on-topic documents reach 0.87–0.90, the 100th-closest sits near 0.84 and
  the 400th near 0.83 (`bitcoin`, the corpus's own subject, runs about 0.02 higher). The first
  guess, 0.78–0.92, put nearly every document halfway up the rung. Read `sem_sim` in the match
  features before you move them.
- An event with no text (an empty kind 1) gets no embedding, so its cosine is NaN. `sem_sim`
  reads it as 0, because a NaN reaching `max()` and the drop limit would poison the page.

Trust still multiplies everything, so a dense hit by an author under `min_rank` is
gated exactly as a lexical one is.

### 2.2 The configs

| config | recall | rung | mult | isolates |
|---|---|---|---|---|
| `sem_mult_1`, `sem_mult_3` | lexical | — | 1, 3 | aboutness reordering inside bands |
| `sem_tier_550` | lexical | 550 | — | the rung lifting weak lexical matches |
| `hybrid_300` / `_550` / `_1000` | + dense, targetHits 100 | 300 / 550 / 1000 | — | where a dense-only hit should sit: under, on and over the body rung (550), all under the weak rung (4000) |
| `hybrid_550_mult_1` | + dense | 550 | 1 | both together |
| `hybrid_4000`, `hybrid_23000` | + dense | 4000 / 23000 | — | diagnostic only: the weak and near rungs, to see what dense recall surfaces once it can reach page one (§7.1) |

## 3. Running it

Everything runs against a **local** Vespa. Never feed the live relay (AGENTS.md).

```bash
# 1. Topic pages + production's run, read-only (search-evaluation.md §8.1)
python3 benchmark/capture_eval.py benchmark/rank_cases.json /tmp/eval
# 2. The query-independent background slice, read-only
python3 benchmark/embedding/capture_background.py /tmp/eval/corpus.json /tmp/eval/background.json
python3 -c "import json; a=json.load(open('/tmp/eval/corpus.json')); i={e['id'] for e in a}; \
  json.dump(a+[e for e in json.load(open('/tmp/eval/background.json')) if e['id'] not in i], open('/tmp/eval/corpus_full.json','w'))"
# 3. The model (the onnx/ folder of the Hugging Face repository)
mkdir -p /tmp/e5 && for f in model.onnx tokenizer.json; do
  curl -sL -o /tmp/e5/$f https://huggingface.co/intfloat/multilingual-e5-small/resolve/main/onnx/$f; done
# 4. The experiment package, deployed BEFORE anything feeds the cluster. open() deploys only
#    when nothing serves (SchemaDeploy.IF_ABSENT), so exportLoad leaves this package alone.
./gradlew :engine:vespaAppZip
python3 benchmark/embedding/build_app.py engine/build/vespa-app/vespa-app.zip /tmp/e5 /tmp/e5app
docker run --detach --name vespa --publish 8080:8080 --publish 19071:19071 vespaengine/vespa
docker cp /tmp/e5app vespa:/tmp/app && docker exec -u root vespa chown -R vespa /tmp/app
docker exec vespa bash -c 'vespa-deploy prepare /tmp/app && vespa-deploy activate'
# 5. Load. Embedding runs at feed time, which costs CPU (§5)
VESPA_URL=http://localhost:8080 ./gradlew :benchmark:exportLoad --args="/tmp/eval/corpus_full.json"
# 6. The A/B, production first as the baseline, and pool what is new
./gradlew :benchmark:rankAb --args="--observer 460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c \
  --run production=/tmp/eval/run_production.json --now <capture instant> \
  --configs production,baseline,sem_mult_1,sem_mult_3,sem_tier_550,hybrid_300,hybrid_550,hybrid_1000,hybrid_550_mult_1 \
  --pool /tmp/eval/pool_embedding.json"
```

`baseline` on this cluster must read like `baseline` on a stock one. The knobs are inert,
so any difference is a bug in the package and not a result. Check it first: the
`baseline` row's rbo against production should match earlier runs.

## 4. Judging, and why this one needs people

`search-evaluation.md` §5.3 decides this. A change that puts a language model into the
ranking is **evaluated on human grades only**. Nothing in the ranking used a model until now.
LLM grades are excluded from the verdict, because the judge's notion of relevance and the
embedding's are too likely to agree for reasons that have nothing to do with users. So:

1. Grade the pool worksheet (`--pool`) **blind**, by hand, against each topic's `need`
   (§4 scale). The dense configs surface documents no lexical config ever did. Until they
   are graded, the condensed nDCG drops them and the gain reads as **zero by construction**
   (§5.1). That is the most likely way for this experiment to lie.
2. `--merge` with `--by human:<you>`.
3. Re-run step 6 and read the verdict block **by stratum**. The bar (§1) is on `topical`, and
   the guard is on `navigational`, `prefix` and `fuzzy`.

## 5. Caveats this setup cannot remove

- **Scale.** About 20k events, against 211M in production. A dense-only hit competes with
  far fewer near-duplicates here, so recall gains read **optimistic**. Rank-only gains
  (`sem_mult_*`) transfer better.
- **The background slice is trusted authors' recent posts.** That is where an on-topic,
  wordless note would come from in production too. Older or less-trusted material is not
  represented.
- **One model.** multilingual-e5-small is the cheap end. If it clears the bar, a larger
  model is the follow-up. If it misses by a little, try the larger one before giving up.
- **Short text.** Most notes are a sentence. Embedding a one-word reply is close to noise,
  which is what `sem_floor` is for. Sweep it if the pages show replies floating up.
- **Feed cost** is real and is not measured here. Every searchable event is one
  transformer pass at ingest.

## 6. What it would cost on production (to read beside the verdict)

| choice | per document | kind 1 alone (132M) |
|---|---|---|
| bfloat16 384-d, HNSW | 768 B + ~130 B links | ~118 GB RAM |
| binarized 384-bit, HNSW (hamming), bf16 paged for re-rank | 48 B + ~130 B | ~24 GB RAM + 100 GB disk |
| re-rank only (`sem_mult`), no HNSW, paged | 768 B on disk | ~100 GB disk, ~0 RAM |

`attribute-memory.md` is the budget to read this against. Re-rank-only is affordable, and
dense recall over notes is not unless it is binarized. That is why the configs keep the two
apart: if `sem_mult_*` alone carries most of the gain, recall is not worth buying.

## 7. Results

### 7.1 First run (2026-11-25), on existing judgments only

Corpus: 11 928 topic events from `capture_eval.py`, plus 8 120 background events (600 most-trusted
authors, at most 40 each). 20 044 loaded, every one embedded at feed time, at about 20
documents a second on 4 cores. Observer `460c25…065c`, `--now 1795617900`. Production is the
baseline. **Every number below scores only documents that were already judged**, and the
condensed list drops the rest (§4). A config whose new documents are all unjudged therefore
reads exactly like `baseline`.

| config | Δ nDCG@10 vs production | 95% CI | W/T/L | topics whose top 10 changed vs `baseline` | new top-10 docs (unjudged) |
|---|---:|---|---:|---:|---|
| `baseline` | −0.006 | [−0.015, +0.002] | 4/39/10 | — | — |
| `sem_tier_550` | −0.006 | same as baseline | 4/39/10 | 0 | 0 |
| `hybrid_300`, `hybrid_550` | −0.006 | same as baseline | 4/39/10 | 1 | 8 (multilingual) |
| `hybrid_1000` | −0.006 | [−0.015, +0.002] | 5/38/10 | 2 | 9 |
| `hybrid_4000` | −0.006 | [−0.015, +0.003] | 6/36/11 | 6 | 25 (10 topical, 13 multilingual) |
| `hybrid_23000` | −0.011 | [−0.023, +0.000] | 5/35/13 | 13 | 64 (34 topical, 20 multilingual) |
| `sem_mult_1` | **−0.031** | [−0.053, −0.009] | 12/14/27 | 46 | 102 |
| `sem_mult_3` | **−0.038** | [−0.068, −0.012] | 18/9/26 | — | — |

**`baseline` reads like production** (−0.006, with the same `Vitor` drift as earlier runs), so
the experiment package is inert at its defaults, as designed.

**Finding 1: at the body rung, dense recall cannot reach page one, because the ladder decides
first.** On a one-word topical query the top 40 is the name rung (~130 000). For `coffee`, that
means every calendar event, place, app and list NAMED "Coffee". A note about coffee is on the body
rung (550), about 236× under it, whether word matching or the embedding recalled it. Recall is
not the binding constraint for topical queries. The name and perfect-match rungs are, as §9 of
`search-evaluation.md` already measured (`perfect_off`: topical +0.126). An embedding cannot
help a topical query until the ladder lets aboutness compete with names on topical-shaped
queries. That is a ranking change of its own, and it comes first.

**Finding 2: a uniform aboutness multiplier hurts.** `sem_mult_*` reorders 46 of 54 topics,
and on judged documents it loses on navigational and ambiguous queries (`alby` −0.42, `amethyst`
−0.29, `Jack` RR −0.80, `iris` RR −0.75 at ×3). The mechanism: a profile's bio is not "about" its
name, while a note mentioning the name is. A multiplier would have to be gated by query intent,
or confined to the body band, before it could ship.

**Finding 3: dense hits that do surface are mixed.** Looked at, not graded, so this is not a
result:
- A real semantic match: `divórcio e desabafa` surfaces a Portuguese note about a cousin
  separating after a 15-year marriage, which contains neither word.
- Short-text noise: the same topic also surfaces "Swipe left & ignore updates", and
  `de extraordinary` surfaces three "welcome to nostr" notes.

A sentence-length note sits close to every query in e5 space, and `sem_floor` alone does not
separate it.

### 7.2 What is owed before a verdict

1. **Human grades for the pool**: 414 rows, the top 20 of every config over all 54 topics, at
   `pool_embedding.json`. §4 explains why these cannot be model grades.
2. With those grades, re-read `hybrid_4000` and `hybrid_23000` by stratum. If their dense hits
   grade well on topical topics, the case is for **an intent-aware ladder plus dense recall**,
   not dense recall alone. If they grade poorly, the short-text noise is the next thing to fix:
   a minimum text length for the semantic rung, or a larger model.
3. Grow the multilingual stratum. Three topics cannot carry a verdict, and they are the ones
   that moved most.

## 8. The intent-aware ladder (2026-11-25)

§7.1 found that the ladder, not recall, decides topical pages. This round makes the ladder
depend on query intent: `query(topical)`, 0..1, set per query. Every topical knob scales with it
and is inert at 0, so a query read as navigational is ranked exactly as shipped. The knobs are
in `build_app.py`'s docstring, and the configs are in `RankAb.SEMANTIC_CONFIGS` (`intent_*`,
`probe_*`).

Two ways to set `topical`:

- **Oracle** (`_intent` 1): the case file's own label, 1 for `topical`. This is the ceiling: no
  classifier does better.
- **Entity probe** (`_intent` 2, `RankAb.entityProbe`): one extra query for kinds 0 and 31990
  under the lens. The query is navigational when a profile or app whose naming field IS the
  query (`perfect_match`) belongs to an author at trust ≥ 30. It agrees with the labels on
  **32 of 54** topics. Its costly misses are topics that a trusted account is also named
  after: `bitcoin`, `nostr`, `crypto`, `art` and `proof of work` read as navigational. Its
  other misses (prefix, fuzzy, multilingual read as topical) cost little here, because the
  knobs that survived (§8.2) touch only the perfect-match rung, which those queries rarely
  reach.

### 8.1 First cut: cap the name rungs. Worse.

`intent_cap_550` / `_4000` cap the first-phase text score for a topical query and drop all
three naming-field precision boosts. On judged documents, topical goes to −0.265 (cap 550) and
−0.011 (cap 4000). Only **15–47%** of a topical top 10 is judged, against 98% for the baseline,
so the number is mostly unjudged documents. The pages are not ambiguous, though. The name rung
holds two different things:

- entities NAMED the word, like an app or a calendar event called exactly "Coffee";
- articles TITLED with the topic, like "On Freedom of Speech" (grade 3).

The cap flattened both. Passing mentions took their place: "noooooo not the coffeeeeeee", a
`nostr:nprofile…` URI, five copies of one quote. The embedding (`intent_hybrid`, ×1 aboutness)
recovered only part of it (topical −0.115), because e5 scores short notes high.

### 8.2 Second cut: drop only the perfect-match rung, for topical queries only

`perfect_match` is what separates the two: it fires when the naming field IS the query, which
an entity named "Coffee" has and an article titled "On Freedom of Speech" does not. §9 of
`search-evaluation.md` measured dropping that rung for every query (`perfect_off`: topical
+0.126, navigational −0.053). Gating it by intent keeps the topical half and avoids the
navigational loss.

| config (vs production; 53 graded topics) | Δ nDCG@10 | 95% CI | W/T/L | topical (24) | navigational (14) | prefix (6) | topical judged@10 | mean grade of judged top-10 docs |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| `baseline` | −0.006 | [−0.015, +0.002] | 4/39/10 | +0.002 | −0.010 | −0.006 | 0.98 | 1.80 |
| **`intent_perfect_off`** (oracle, no embedding) | **+0.023** | [−0.003, +0.052] | 16/25/12 | **+0.068** | −0.010 | −0.006 | 0.76 | **1.90** |
| `intent_perfect_off_mult` (+ aboutness ×1) | −0.018 | [−0.032, −0.004] | 9/25/19 | −0.023 | −0.010 | −0.006 | 0.84 | 1.74 |
| `intent_perfect_off_hybrid` (+ dense recall + semantic rung) | −0.018 | same as `_mult` | 9/25/19 | −0.023 | −0.010 | −0.006 | 0.84 | 1.74 |
| `probe_perfect_off` (the probe, no embedding) | +0.010 | [−0.011, +0.033] | 13/28/12 | +0.030 | −0.010 | −0.016 | — | — |
| `probe_perfect_off_hybrid` | −0.016 | [−0.032, −0.001] | 11/23/19 | −0.012 | −0.009 | −0.050 | 0.89 | 1.76 |

What it says:

1. **The gain is the intent-aware ladder, and it needs no embedding.** `intent_perfect_off`
   lifts topical +0.068 and leaves every other stratum exactly where `baseline` has it. Its
   judged documents grade higher than the baseline's (1.90 against 1.80), so the unjudged
   quarter of its top 10 is not hiding the gain.
2. **On top of it, the embedding subtracts.** The aboutness multiplier reorders 22 topics,
   costs topical about 0.09 against `intent_perfect_off`, and lowers the judged grade
   (1.74). Dense recall adds **nothing**: with names no longer capped, a dense-only hit on the
   body rung never reaches a top 10 (`_hybrid`'s pages equal `_mult`'s on every topic). The
   short-text bias seen in §7.1 is the likely mechanism: e5-small puts a one-line mention
   close to the query.
3. **The probe keeps under half of the oracle's gain** (topical +0.030 against +0.068),
   because it reads `bitcoin` and `nostr` as navigational. A better classifier is worth up
   to the difference.

### 8.4 Under production's name statistics (step 1)

Production damps the perfect-match rung by how common the query word is as a name
(`perfect_share()`). On the production sample (`benchmark/name_df.json`), that takes the rung
fully off `bitcoin` (6.7e-4) and `nostr` (6.4e-4), and partly off `ai`, `music`, `news` and
`crypto`. Multi-word queries are never damped. An evaluation index is under
`perfect_df_min_docs` (100 000), so §8.2 ran with the damping OFF, and part of its gain could
have been something production already does. The `prod_*` configs lift the guard and run with
`--name-df benchmark/name_df.json`.

| vs production | Δ nDCG@10 | 95% CI | W/T/L | topical |
|---|---:|---|---:|---:|
| `baseline` (damping off) | −0.006 | [−0.015, +0.002] | 4/39/10 | +0.002 |
| `prod_baseline` (damping as production) | −0.001 | [−0.012, +0.010] | 8/36/9 | +0.015 |
| `prod_intent_perfect_off` | +0.023 | [−0.003, +0.052] | 16/25/12 | +0.068 |
| `prod_probe_perfect_off` | +0.015 | [−0.007, +0.038] | 16/26/11 | +0.041 |

`prod_baseline` is the faithful local replay of production (−0.001), so it is the right baseline
for the candidate's own effect. Paired against it (`--run` files, same judgments):

| vs `prod_baseline` | Δ nDCG@10 | 95% CI | W/T/L | topical (24) | every other stratum |
|---|---:|---|---:|---:|---|
| **candidate** (`prod_intent_perfect_off`, oracle) | **+0.024** | **[+0.002, +0.049]** | 12/34/7 | **+0.053** | 0.000 |
| probe (`prod_probe_perfect_off`) | +0.015 | [−0.004, +0.036] | 11/33/9 | +0.026 | ambiguous +0.089, prefix −0.010, multilingual −0.005 |

Production's damping accounted for about 0.015 of §8.2's +0.068, and the candidate's own share
is **+0.053 topical**. The CI's low end clears −0.01. By construction, it leaves every non-topical
topic exactly as it was.

**The two named losses have one mechanism: the reference work titled exactly the topic.**

- `the art of war` (nDCG −0.17, RR −0.30): the wiki article "The Art of War" (kind 30818, grade 3)
  goes from #2 to #5, below articles that only mention war.
- `pico 8` (RR −0.50): the wiki article "PICO-8" (grade 3) leaves the top 6.

On a topical query, the perfect-match rung pays two different things. One is an ENTITY named the
topic: an app or calendar event called "Coffee", graded 1–2, which is what the rung should stop
paying. The other is the canonical REFERENCE document about it: a wiki page or long-form
article whose title is the topic, graded 3. Dropping the rung for topical queries drops both.

**Next refinement:** drop it only off entity kinds, and keep it on reference kinds (30818 wiki,
30023 long-form). `attribute(kind)` makes that one expression. It should remove both losses
without giving back the `coffee`-style gains, whose perfect matches are apps (32267), calendar
events (31923) and lists. Then grade the candidate's unjudged rows (`pool_step1.json`: 190 rows
over six configs, fewer for the candidate alone) and read it against the ship rule.

### 8.5 The rung kept on reference documents: the candidate

§8.4's two losses were wiki articles titled exactly the topic. `topical_keep_reference` keeps the
perfect-match rung on wiki (30818) and long-form (30023) documents and drops it only off
everything else (apps, calendar events, lists, profiles), and still only for a topical query.
Same cluster, `--name-df`, paired against `prod_baseline` (whose top 10 is identical, on 54 of
54 topics, to its run before the machine restarted):

| vs `prod_baseline` | Δ nDCG@10 | 95% CI | p (Holm) | W/T/L | topical nDCG (24) | topical RR (22) | other strata |
|---|---:|---|---:|---:|---:|---:|---|
| `prod_intent_perfect_off` (§8.4) | +0.024 | [+0.002, +0.049] | 0.053 | 12/34/7 | +0.053 | +0.027 | 0.000 |
| **`prod_intent_entity_off`** (oracle) | **+0.051** | **[+0.023, +0.083]** | **0.003** | **14/38/1** | **+0.113** | **+0.225** | 0.000 |
| **`prod_probe_entity_off`** (probe) | **+0.039** | **[+0.015, +0.067]** | **0.004** | 13/37/3 | +0.079 | +0.164 | ambiguous +0.089, prefix −0.010, multilingual −0.005 |

- **No loss over 0.1** for either. `the art of war` and `pico 8` are back: their wiki pages are
  #2 and #1.
- **The judged evidence agrees.** The candidates' judged top-10 documents grade higher than the
  baseline's (2.03 and 1.97 against 1.81), with 78% of a topical top 10 judged (baseline 88%).
- **The one `LOST` case is not this change.** `Verified Human` is navigational, so its pages
  are untouched, and its pinned document (production's #81) is missing from every local
  config, `prod_baseline` included.
- **For scale:** the phrase band, the last ranking change shipped, measured +0.040 overall and
  +0.079 topical. The probe version matches it, and the oracle doubles its topical gain.

**Owed before shipping:** 52 unjudged rows across the two candidates' topical top 10s
(`pool_step2.json` also holds their top-20 rows and the baseline's). These may be model-graded
with a 20% human audit (§5.3), because the change adds no model. Then the probe's cost on the
query path (one extra kind-restricted query per search, not yet measured) and the shipping
work (§8.6).

### 8.6 What shipping it takes (none of the embedding)

1. `event.sd`, `search` profile: `query(topical)` (default 0) and the perfect-match subtraction
   gated by `reference_kind()`. Inert at 0, so it deploys ahead of the store change.
2. The store decides `topical` (the entity probe, or a better classifier) and sends it through
   `EventQuery.rankFeatures`, as `LookupQueries` already does for member scoring.
3. A ranking IT that pins it: `coffee` reaches articles, `the art of war` / `pico 8` keep their
   wiki page first, and `odell` / `amethyst` do not move. `RankRegressionIT`, `PhraseBandIT` and
   `FieldCoverageRankIT` stay green.
4. After deploying, re-capture production and confirm that the live pages match the local
   replay (§9).

## 9. A broad regression set, and the probe calibrated on production

### 9.1 The set

`benchmark/rank_cases_broad.json` (`benchmark/embedding/make_broad_cases.py`) holds 147 queries
written without looking at any page, all `split: broad`:

- 30 people (20 full names, 10 one-word handles such as `fiatjaf`, `jb55`, `Gigi`);
- 20 apps, several named with an ordinary word (`snort`, `ditto`, `olas`, `phoenix`);
- 40 one-word and 25 multi-word topics;
- 12 as-you-type prefixes;
- 20 non-English words.

Hints came from the trusted names, apps and hashtags in the eval capture. No search log was
used: the relay's term log is admin-only and quotes what people typed. The pages were captured
read-only (30 439 events, 28 247 new) and loaded beside the §7 corpus (48 284 in all).

### 9.2 What production changed about the probe

**The local corpus flattered it.** Run on production (`benchmark/embedding/probe_decisions.py`,
REQ only), the first probe, "a trust-30+ profile or app named the query", read **18 of 89**
topical queries as navigational: `bitcoin`, `nostr`, `coffee`, `art`, `privacy`, `crypto`,
`freedom`, `science`, `philosophy`, `sound money`, and more. The local capture did not hold the
accounts involved. Each is someone named after the word ("Coffee ☕", "art", "Science", the
shitposter), and trust saturates: they sit at 75–100 like everyone real. **Followers separate
them.** The navigational entities run to thousands (ODELL 26 041, Gigi 25 187, damus 16 928,
the apps 1 000–10 000), while the accounts named after a topic run to a few hundred ("nostr" 220,
"art" 275, "privacy" 124). The shipped rule is an app handler, or a profile with **at least
1 000 followers** under the lens. With it, production reads 87 of 89 topical queries topical
(the misses are `coffee` at 1 842 and `memes` at 3 451). 52 of 65 navigational queries read
navigational, and most of the other 13 hold no profile named exactly the query, so the rung the
intent moves never touched them.

**Cost.** Timed on production, 200 queries, wall clock, each REQ paying about 56 ms of fixed
overhead (`benchmark/embedding/probe_latency.py`):

| REQ | p50 | p95 |
|---|---:|---:|
| the search a client sends | 929 ms | 3 989 ms |
| the probe sent as words | 342–429 ms | 1 292–2 089 ms (`dell` 4.2 s) |
| **the probe sent as one quoted phrase (shipped)** | **77 ms** | **653 ms** |

"Named exactly" needs no fuzzy, prefix or trigram reach. The phrase made the same decision as the
word form on 198 of 199 queries, and its only miss (`nostr`) is fixed by reading 50 deep instead
of 20. Inside the store, the probe is one engine search with no network hop and no reference
expansion, so it costs less than this table shows. Its answers are cached for ten minutes per
lens and text.

### 9.3 The verdict, graded

958 rows were graded blind by six parallel model assessors (provenance `llm:claude`), grouped so
that each topic is graded by one assessor. They cover the union of the baseline's and the
candidate's top 10 on every broad topic that changed, plus the 81 owed rows of the 54-case run.
That is allowed here because the ranking holds no model (§5.3). The blind 20% audit sheet a
person still owes is `benchmark/search_judgments_audit_broad.json` (203 rows over 96 topics).
Every topic is ranked under **production's** probe decision (`rankAb --intent-file`, `_intent` 3).

**The 147 broad queries.** Damping is off on both sides, since `name_df.json` does not cover
these words. 71 topics changed; the other 76 are identical.

| candidate vs baseline (71 changed topics) | Δ nDCG@10 | 95% CI | W/T/L | topical (55) | multilingual (10) | navigational (5) | prefix (1) |
|---|---:|---|---:|---:|---:|---:|---:|
| `file_entity_off` | **+0.176** | [+0.131, +0.223] | 57/5/9 | **+0.215** | +0.078 | −0.019 | −0.018 |

That averages about **+0.085** across all 147. RR: +0.222 [+0.142, +0.304]. **The one loss over
0.1 is `Derek Ross`** (RR −0.50). His profile has 659 followers, under the floor, so the query
reads topical and the profile loses the rung while his own long-form "AMA" article, a reference
kind, keeps it: profile #2, article #1. The cost of the follower floor is a person with a small
following whose own article is titled their name. The other navigational changes are within a
rank or two (`Joe Carlasare`'s two calendar events swap; `mutiny`'s page is wrong in both, led by
a brewery named Mutiny).

**The 54 original cases**, under production's decisions and statistics, fully graded:

| vs `prod_baseline` | Δ nDCG@10 | 95% CI | p | W/T/L | topical (24) | RR |
|---|---:|---|---:|---:|---:|---:|
| **probe, as shipped** (`prodfile_entity_off`) | **+0.056** | **[+0.027, +0.088]** | <0.001 | 16/36/2 | **+0.123** | +0.113 |
| oracle (`prod_intent_entity_off`) | +0.054 | [+0.025, +0.086] | <0.001 | 14/39/1 | +0.121 | +0.118 |

With the follower rule, the probe reaches the oracle. No loss over 0.1.

## 10. What shipped

- **`engine/app/schemas/event.sd`**, `search` profile: `query(topical)` (default 0),
  `reference_kind()` (30818, 30023) and `topical_perfect_drop()`, subtracted in the second phase
  and surfaced as a match-feature. At 0, the ranking is exactly what it was. An older schema
  ignores the input it does not declare (checked on Vespa: an undeclared
  `ranking.features.query(x)` is accepted and unread), so a new store on an old cluster ranks as
  before.
- **`engine`**: `EventQuery.topical`, which `EventYql` sends on `search` only and only when
  positive. `Ranked.followers` carries the `verified_followers` match-feature.
  `text/EntityNames` is the one "named exactly" test, shared by the store and `rankAb`.
- **`store/search/QueryIntent`**: the probe (one quoted phrase over kinds 0 and 31990, min_rank
  30, 50 deep, under the reader's lens), the follower floor, a ten-minute cache, and fail-open
  behaviour. It runs only on the two paths that serve a ranked page (`NostrSemanticsStore.pageOf`,
  `rawQuery`). Counts and gates skip it. So do profile-only, notes-only and reference-only reads.
- **`EngineTuning.intentProbe` / `VESPA_INTENT_PROBE`**: on by default. `false` restores the old
  ranking and removes the round trip.
- **Tests**: `EntityNamesTest`, `QueryIntentTest`, `EventYqlTest`, `EngineTuningTest`, and
  `TopicalIntentIT`, which runs the store's own read path on 55 signed production events
  (`lightning` led by an article instead of the sets titled Lightning; `the art of war`'s wiki
  page kept in the top 2; `odell` untouched; and the old order back with the probe off).
  RankRegressionIT, PhraseBandIT, FieldCoverageRankIT and the unit suites stay green.
- **Not shipped**: the embedding package, the semantic rung and the aboutness multiplier
  (`benchmark/embedding/build_app.py`, `RankAb.SEMANTIC_CONFIGS`). They remain an experiment.

## 11. Should a publication (30040) be a reference kind? No.

On the graded mixed pages (§9, §10), wiki (30818, mean grade 2.80 when titled exactly a topical
query) and long-form (30023, 2.67) are the only kinds whose exact-title documents are reliably the
answer. Lists and sets grade 1.0–1.3, profiles 0.5, and nothing else clears 1.9. Several kinds that
look like reference documents never reached a graded page, for three reasons, all checked on
production (read-only COUNTs and REQs):

- **Drafts (30024): 0 served.**
- **Too rare to rank:** NIP documents (30817) 479, learning resources (30142) 699, podcast episodes
  (54) 855 and (30054) 176, hardcopy citations (32) 4, against 1.1M long-form and 6M wiki.
- **Publications (30040 / 30041): 99 041 / 2.5M, but off the page.** 126 of 133 sampled publications
  matching common topics come from one public-domain mirror (GC-Publishing) the canonical lens trusts
  at 32 with 36 followers. On the convex trust curve that is roughly ×25 under a trust-100 author on
  the same rung, so no publication reaches a mixed top 150.

So the place a 30040 rule could matter is a publications-only read. `benchmark/rank_cases_publications.json`
(`make_publication_cases.py`) holds 25 book titles (classics the mirror carries, plus books by authors the
lens trusts at 70+: the US Constitution, *Interior Castle*, *Mysticism and Logic*) and 12 topics, all as
`kinds: [30040]`, plus 8 of the titles on the mixed page. `capture_eval.py` now captures a case's
`kinds`. 3 660 events were captured and 3 297 new ones loaded. Production's probe reads every book title
as topical, since no profile or app is named "Pride and Prejudice", so the shipped rule applies to all of
them. The experiment package gained `query(ref_30040)`, which widens the shipped `reference_kind()`.

| change | publication-tab pages changed (37) | mixed pages changed (8) |
|---|---:|---:|
| baseline → shipped | **0** | 1 (`Common Sense`) |
| shipped → shipped + 30040 as reference | **0** | **0** |

Why nothing moves:

- **Exact titles are rare.** 31 of the 251 publication-tab top-10 hits are titled exactly the query;
  `The Republic` has no publication titled exactly that.
- **Where one exists, it leads either way.** On a single-kind page (and mostly one author), the
  exactness boost ("the query is the whole title") still ranks the exact title first after the
  perfect-match rung leaves: "Pride and Prejudice" and Dikaios1517's "Bitcoin" are #1 in all three.

The one page the shipped rule moves is an improvement, though it was not graded: `Common Sense` on the
mixed page goes from a song and a profile named "Common Sense" at #1–2 to a book-club event about
Paine's *Common Sense*. The wiki page on the pamphlet holds #6 throughout.

**Verdict: leave `reference_kind()` at 30818 and 30023.** Adding 30040 changes no page. Revisit only if
trusted authors publish books in volume (the convex trust curve, not this rung, is what keeps
publications off mixed pages), and re-run this set when they do.

### 10.1 Still open

- **The human audit** of the 958 model grades (§9.3), `benchmark/search_judgments_audit_broad.json`:
  run `rankAb --agreement` on it before anyone `--merge`s it (§5.3).
- **The follower floor's cost**: a person under 1 000 followers whose own reference article is
  titled their name (`Derek Ross`, §9.3). A narrower rule to try is to keep the rung on any
  profile named the query when the page also holds that profile's own reference document.
- **The probe's two topical misses** (`coffee`, `memes`: an account named the word with
  1 842 / 3 451 followers). Production's name-frequency damping already covers the commonest
  words, and these two are not.
- **The embedding, if revisited:** a larger model, a minimum text length or a substance prior
  before the aboutness signal, and grading the §7 pool. As built, it does not earn its RAM (§6).
