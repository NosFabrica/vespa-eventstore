# The embedding experiment: does dense retrieval improve search?

Status: **harness built, first run done, pool unjudged (414 rows).** Nothing here ships. The experiment
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
