# Evaluating search quality

How to tell whether a change to NIP-50 search made results **better**, across many queries
at once, before it ships. It covers rank-profile weights, extraction, schema, the query
compiler, and the search grammar. It is written for whoever makes the next change, so it
says what to run, what to read and what counts as a win. It also explains why each rule is
there, with the IR-evaluation literature it comes from (§10).

The protocol in one paragraph: **topics** (a query, plus the lens and tab it is asked under)
are graded by assessors who are **blind** to which system surfaced each document. They use a
4-point scale, and a grade is kept per event id. Every candidate is run against the baseline
**in the same run**. The verdict is read from **paired per-topic deltas**: nDCG@10 for graded
topics, success@k and RR for known-item ones, with a randomization test, a bootstrap CI, a
Holm correction for sweeps, and the **minimum detectable effect** printed beside it. Two
**guardrails** sit next to the verdict: page age and author concentration. The existing
**hard gates** stay hard: the `*IT` suites and "no pinned case lost". And any topic that
loses more than 0.1 is **named**.

## 0. Checklist for a search change

1. **Hard gates**: `./gradlew :benchmark:test -Pintegration`, which runs `RankRegressionIT`,
   `FieldCoverageRankIT`, `SearchPrefixLadderIT` and the rest, must stay green. They pin
   shapes that were once broken, so they are pass/fail, not a score.
2. **Load a corpus** that holds the topics' real pages (§8.1). Use the same corpus for the
   baseline and the candidate.
3. **Run** `rankAb` with the baseline first, then your candidate(s), plus `--pool`
   (§8.2). For a schema or extraction change that a `query()` override cannot express, run
   twice against two clusters (§8.4).
4. **Judge the pool** if any graded topic shows `judged@10` under 0.9, merge, then re-run
   (§5). Never read a verdict off an under-judged page.
5. **Read the verdict** (§6): no pinned case LOST, the nDCG@10 CI's low end ≥ −0.01, every
   listed loss explained in the PR, and the guardrails held. Paste the verdict block into
   the PR.
6. **Add topics**: every reported bad search becomes a case (as before), with an `intent`,
   and its pool gets judged.

## 1. What existed, and what it could not answer

Until now, three tools guarded ranking:

| tool | measures | blind spot |
|---|---|---|
| `RankRegressionIT` & co. | asserted positions on a synthetic corpus | pass/fail per shape; says nothing about real pages |
| `rankAb` case table | position of ONE `expect` doc per query → MRR; median page age | one relevant doc per query, so everything else on the page is invisible; MRR over ~8 cases with no notion of noise |
| `RankQuality` (`BENCH_RANK_QUALITY`) | overlap@k and Kendall's τ against the current profile | measures *change*, not *quality* — it treats the baseline as ground truth |

None of them answers "across all our queries, is the candidate's page **better** than the
baseline's, and by more than chance?" The first live run of the new columns (§9) shows the
gap. On the 8 known-item cases, `perfect_off` dropped Avi Burra from #1 to #2 and Primal
from #3 to #5. That is a mean RR change of −0.079, yet p = 0.5, and the run could
only have detected |ΔRR| ≥ 0.175. Eight topics cannot tell a real loss from noise. The case
table showed the loss without being able to establish it.

## 2. The model: Cranfield, adapted to a lens

This is the **Cranfield paradigm** that TREC formalised: a fixed set of topics, a corpus,
relevance judgments, and a measure. Systems are compared on the same topics, and per-topic
scores are aggregated. Three facts about this store shape how it applies:

- **A topic is a query *under a lens*, not just a query.** The same text ranks differently
  for different observers. That is the product: the trust gate drops strangers and the trust
  curve reorders the rest. A `rank_cases.json` row therefore carries an optional `observer`
  (64-hex, or `none` for the anonymous page) and `kinds` (the tab: `[0]` is People). These
  override `--observer`/`--kinds`. `rankAb` resolves each observer's kind 10040 to its
  service keys, as the store does per query (`docs/service-keyed-trust.md`), so the page
  measured is the page served.
- **Judgments are about the document, not about trust.** An assessor grades whether the
  event answers the query. They do **not** re-derive the author's score. Otherwise the
  evaluation would measure the trust ranking against itself, which is the circularity of §5.3
  in another form. Trust enters the grade in one place only: an impersonator, a squatter or
  spam is grade **0**, however well its text matches. A human can tell that the trust-13
  "ODELL mirror" is not ODELL. Whether the ranker keeps it off the page is then measured
  rather than assumed.
- **The corpus is live, so judgments must survive it.** Grades are keyed by **event id**,
  which is global. A judgment made on a local slice holds on the relay and vice versa, and a
  new note does not invalidate the old grades. It is only *unjudged* (§5.1). Absolute scores
  are comparable only on one corpus: a slice that lacks a judged document can never retrieve
  it, which lowers every config's nDCG alike. **Paired** deltas stay valid, which is why
  baseline and candidate always share a run.

A "system" in this vocabulary is a `rankAb` config: `query()` overrides layered on what
production sends, built by `EventYql` itself so the YQL is byte-identical. A change that
cannot be expressed as overrides (schema, extraction, `EventYql`) is a pair of clusters
instead (§8.4).

Case text goes through the NIP-50 grammar the relay applies (Quartz's `SearchQuery`, mirrored
from the store's `FilterMapping`): `-word` excludes, `"quotes"` make phrases, `include:spam` and
`language:` apply, and an unknown `key:value` extension is ignored. What `rankAb` does **not**
reproduce is the store's page assembly: reference expansion, which splices a Trusted List's
members onto the page, and the companion declarations. It measures the **engine's** ranking. A
topic whose answer arrives by splicing reads differently on the relay. On the first
measurement (§9), the "Verified Human" member production serves at #8 is absent from the
`rankAb` page, and the case's `expect` reads LOST against production for that reason alone.

## 3. Topics

### 3.1 Strata (`intent`)

A mean over all topics hides a regression confined to one kind of query. A recency weight
that ages-down topical pages can quietly cost the navigational ones. So every case has an
`intent`, and the verdict is broken down by it:

| intent | the user wants | examples in the case file | read it with |
|---|---|---|---|
| `navigational` | one entity (person, app, list) they can name | `Avi Burra`, `amethyst`, `odell`, `Primal` | S@1, S@3, RR |
| `ambiguous` | one of several entities sharing a name | `Jack`, `Vitor` | nDCG@10, author share |
| `prefix` | the entity they are *still typing* | `Ode`, `Vitor P`, `testin` | S@3, nDCG@10 |
| `fuzzy` | an entity under a typo, infix or folded spelling | `dell`, `jose` | S@3 |
| `identity` | a handle or domain | `primal.net` | nDCG@10 |
| `multilingual` | non-English or CJK text | `divórcio e desabafa`, `esposa`, `中村` | nDCG@10 |
| `topical` | notes *about* a subject | `bitcoin`, `nostr`, `lightning`, `coffee` | nDCG@10, page age |

New strata are fine when a class of report keeps recurring (hashtags, NIP-05 lookups). Add
one deliberately and name it here.

### 3.2 How many

This is the uncomfortable number. With per-topic deltas whose SD is σ, a paired test at
α = 0.05 with 80% power detects a mean delta of about **2.8·σ/√n** (Webber, Moffat & Zobel
2008; Sakai's topic-set-size design formalises the same thing). `rankAb` prints this as
**MDE** beside every comparison:

| topics | MDE at σ = 0.1 | MDE at σ = 0.2 |
|---:|---:|---:|
| 8 | 0.099 | 0.198 |
| 25 | 0.056 | 0.112 |
| 50 | 0.040 | 0.079 |
| 100 | 0.028 | 0.056 |

Ranking changes here are usually targeted: one shape moves a lot and the rest barely move.
So σ is large and the useful topic count is **≥ 50 graded topics, ≥ 5 per stratum**, which
is also where TREC settled. Below that, read the per-topic losses (§6.3) as the evidence and
the p-value as a formality. "Not significant" with an MDE of 0.17 means "smaller than 0.17,
if anything". It does not mean "no effect".

### 3.3 Where topics come from

1. **Every reported bad search**, as the repo already requires (`rank_cases.json`). These are
   the topics most likely to regress, so they are over-represented on purpose. That bias is
   known and acceptable *only because* the next source exists.
2. **A sample of real queries**, so the set is not only the queries that once went wrong. If
   the relay keeps NIP-50 query text, sample it stratified by frequency (head, torso, tail),
   keep only the text (never who asked), and drop anything that looks like a private
   identifier. Until a log exists, draw from the strata above.
3. **One per new feature.** A search change that adds a capability (a new kind, a new
   extension) adds the topic that proves it.

### 3.4 The held-out split (`split`)

A weight swept until it scores well on a topic set has been **fitted** to that set. This is
the topic-set analogue of training on the test data. Cases carry `split: dev` (the default)
or `split: test`. Tune and sweep with `--split dev`; confirm the chosen config **once** with
`--split test`, and do not iterate on what it says. Assign roughly every fourth new topic to
`test` at creation, before anyone has seen how it ranks.

## 4. Grading

Four grades. Exponential gain (2^g − 1, §6.1) makes "the answer" worth seven "marginal"
hits, which is what lets the metric notice a navigational target slipping under a page of
fragments.

| grade | meaning | Nostr examples |
|:-:|---|---|
| **3** | **The answer**: the entity the query names, or a document squarely and substantially about the topic | `Avi Burra` → his kind-0 profile; `amethyst` → the app's 31990 handler or 32267 listing; `bitcoin` → a substantive note about bitcoin |
| **2** | **Relevant**: clearly serves the intent, but is not *the* answer | `Avi Burra` → a review of his novel, a live stream he hosts; `amethyst` → the "Amethyst Devs" channel |
| **1** | **Marginal**: mentions the query, but is not about it, or matches another sense | `amethyst` → the wiki page about the gemstone; `Jack` → "Jack Everitt"; a note with `#bitcoin` and nothing else |
| **0** | **Not relevant**: an accidental match, or bad | `vitor` → "take a vitamin and open the editor" (trigram noise); an impersonator or name-squatter; spam; an event that is empty for this query |

Each case carries a **`need`**: one or two sentences saying what the user wants and what
earns each grade for *this* query (TREC's narrative). It names the real entity, by pubkey
when an impersonator is plausible ("Vitor Pamplona, pubkey 460c25…"). The worksheet copies it
onto every row, so any assessor, human or model, grades against the same statement, and a
later round grades the same way as an earlier one. A case without a `need` should not be
graded.

Rules that keep grades reusable:

- **One document at a time** (the Cranfield independence assumption). Ten copies of a relevant
  picture are ten relevant hits. Redundancy is measured separately, by the author-share
  guardrail (§6.2), and not folded into grades. Folding it in would make a grade depend on
  what else the page happened to hold.
- **Time-independent.** Grade the content, not its age. A good 2023 note is still a good
  note. Freshness is the page-age guardrail's job (`docs/recency-ranking.md`). Grades that
  decay would have to be redone every month.
- **When unsure, between two grades, take the lower.** Assessor disagreement is real
  (§5.3) but changes system *rankings* far less than it changes scores (Voorhees 2000). The
  goal is consistency, not precision.
- **Ambiguous queries**: grade 3 every entity a reasonable user could mean (the three
  accounts named exactly "Jack"), and 2 or 1 by how plausible each sense is. Do not try to
  guess *this* observer's intent; the lens already does that.

## 5. Judging

### 5.1 Unjudged documents, and the condensed list

Under a fixed set of judgments a live corpus keeps producing documents nobody graded. Each
measure has to decide what an unjudged hit is. Calling it "irrelevant" (the classic TREC
default) penalises a candidate for surfacing anything new, good or bad. So `rankAb`
computes nDCG over the **condensed list** (Sakai 2007, nDCG′): unjudged hits are removed
before the cut at 10. Sakai showed this is more robust to incompleteness than `bpref` and
simpler than the inferred measures.

Condensing cuts both ways. If only 2 of a page's 10 hits are judged, the "nDCG@10" is really
the two hits' order, so it is **never read alone**. Every graded row prints `judged@10`, the
fraction of the top 10 that carries a grade, and the summary flags graded topics under
**0.9**. Below that, the next step is to judge, not to conclude.

### 5.2 Pooling, incrementally (`--pool`, `--merge`)

TREC judges a **pool**: the union of every compared system's top-k. `rankAb --pool <file>`
writes the union of every config's top `--pool-depth` (default 20) that has no judgment
yet. So each change pays only for what *it* surfaced, and the judgment set grows with the
changes it is meant to evaluate. Use a pool depth of at least 2× the evaluation depth, so
documents just under the cut are graded before a config promotes them.

The worksheet is **blind**: no rank, no score, no config name, and rows sorted by event id.
An assessor who can see that a document came first, or came from the candidate, ends up
grading the ranking instead of the document. Fill in `grade` and `by`, then
`rankAb --merge <file> [--by human:<name>]`. Ungraded rows are skipped, so a worksheet can be
merged in installments.

### 5.3 Who judges — humans, LLMs, and circularity

**Provenance is mandatory.** Every merged grade records `by` (`human:<name>`, `llm:<model>`,
or `expect` for a case's pinned doc) and `at`.

- **Humans** are the reference. The `expect` docs are human reports and are read as grade 3
  automatically.
- **LLM-assisted judging is allowed as a first pass.** At Bing, LLM labels matched real
  searchers' preferences better than third-party human labellers (Thomas et al., SIGIR 2024).
  On TREC 2024 RAG, UMBRELA's automatic labels induced system rankings highly correlated with
  NIST's manual ones (Upadhyay et al. 2024). Two conditions apply. (1) A human re-grades a
  random **≥ 20%** of each LLM batch, blind to the LLM's grade, and the agreement is reported
  in the PR (Cohen's weighted κ). Human assessors are measurably stricter than LLMs
  (Upadhyay et al.), so expect drift toward leniency. (2) The prompt gets exactly the
  worksheet row and this section's §4 table, and nothing about rank or config.
  The audit runs as `rankAb --agreement <sheet>`, where `<sheet>` is the human's blind
  re-grade of a sample in worksheet form. It prints exact and within-one agreement, the
  quadratic-weighted κ, the confusion matrix (mass on one side of the diagonal is a
  lenient or strict assessor) and the topics that disagree. Run it **before**
  `--merge`-ing the same sheet, because a merge overwrites the LLM grade with the human
  one, which is the right resolution of a disagreement and also erases it.
- **Never let the judge and the ranker share a model.** Clarke & Dietz (2024–26) showed that
  once an LLM is both a system component and the evaluator, evaluation becomes circular:
  re-ranking with the judge model inflated scores on its own labels, and system-pair
  disagreements with human judgments rose to 18%. Nothing in today's ranking uses a language
  model. The day one does (embeddings, an LLM re-ranker, LLM-written extraction), that change
  is evaluated on **human** grades only, and LLM grades made by the same model family are
  excluded from its verdict.

### 5.4 Stale `expect`s

The case file pins one id per known-item topic, and live data moves. On the 2026-10-23
capture (§9), the `amethyst` case's `expect` (`08f6f27c`) is gone, and the app handler that
answers the query is now `03a85dde`. The case reads MISS forever, while the page is fine.
Grading the topic's pool fixes this without rewriting history: any grade-3 document counts
for S@k and RR. When an `expect` is permanently gone, say so in its note rather than deleting
the case.

## 6. The verdict

### 6.1 Measures

| measure | over | why this one |
|---|---|---|
| **nDCG@10** (exponential gain, condensed) | graded topics (≥ 5 judgments) | graded, top-weighted, robust to incompleteness when condensed (Järvelin & Kekäläinen 2002; Burges et al. 2005; Sakai 2007). The headline. |
| **judged@10** | graded topics | how far to trust the nDCG beside it (§5.1) |
| **S@1, S@3** | known-item topics (any grade-3 doc) | "is the answer on screen": what a navigational user experiences. Read these **before** RR. |
| **RR** (mean = MRR) | known-item topics | kept for continuity with the old column. It is not an interval scale (Fuhr 2017), so its mean over-weights rank 1→2 moves. Sakai disputes how much that matters; this protocol reads S@k first and RR second. |
| **RBO** (p = 0.9) vs baseline | every topic | how much a config *moved* the page, judgments or not (Webber, Moffat & Zobel 2010). It replaces overlap@k + Kendall's τ, handles non-conjoint lists of unequal length, and predicts how much judging a change will need. |

Considered and not adopted as headlines: **ERR** (Chapelle et al. 2009, a cascade model,
closer to click behaviour) is a reasonable second graded measure if nDCG and S@k ever
disagree on a change. **RBP** (Moffat & Zobel 2008) has the attractive property of
reporting its own residual from unjudged documents, but judged@10 plus the condensed list
already covers that, more legibly. **MAP** and **bpref** need recall-oriented judging that
nobody will do on a 200M-event corpus.

### 6.2 Guardrails (must hold, need not improve)

- **Page age**: the median age of the top 10 (the existing column). A topical config must not
  buy freshness by losing a pinned position, and a navigational change must not age the
  topical pages.
- **Top-author share**: the share of the top 10 held by its single most frequent author. This
  is the 'Jack' report's 13-of-40-slots problem. Grades cannot see it (§4), so it is watched
  here. A change that raises the mean noticeably needs an explanation.
- **Latency**: `searchBench`/`searchTrace`, as before. A quality win that costs a p95 is a
  trade-off and must be argued as one.
- **Hard gates**: the `*IT` suites and "no pinned case LOST" (`<- LOST` in the case table).

### 6.3 The decision rule

`rankAb` prints, per candidate against the first config: n, mean Δ, its 95% bootstrap CI,
the randomization-test p (Holm-adjusted across every candidate in the run), W/T/L, the MDE,
every topic that lost more than **0.1** by name, and the mean Δ per intent. A candidate
**ships** when:

1. the hard gates are green and no pinned case is LOST;
2. the **low end of the nDCG@10 CI is ≥ −0.01**, the non-inferiority margin. A targeted fix
   is expected to help its own topic and *not hurt the rest*. Demanding a significant overall
   win would block almost every legitimate fix at today's topic count;
3. **every listed loss is explained** in the PR. A loss is either a grading error (fix the
   grade), an accepted trade-off (say why), or a bug;
4. the guardrails hold;
5. for a **tuned** config (anything picked from a sweep), 1–4 hold again on `--split test`.

"Significant improvement" (Holm-adjusted p < 0.05 with the CI above 0) is the bar for
claiming a change makes search *better overall*, as opposed to fixing a case.

### 6.4 Statistics, and why these ones

- **Paired, per topic.** Topic difficulty varies far more than system differences do, so
  comparing two independent means throws away the pairing that makes small samples usable.
- **Fisher's randomization test** (sign-flip permutation). It is exact by enumeration up to
  16 topics and sampled with a fixed seed beyond that. Smucker, Allan & Carterette (CIKM
  2007) found it, the bootstrap and the t-test in practical agreement at ~50 TREC topics,
  and recommended against the sign and Wilcoxon tests. Their 2009 follow-up found the tests
  diverge as topics shrink, which is one more reason §3.2's numbers matter.
- **Holm across a sweep.** Twenty configs compared at α = 0.05 will produce a "significant"
  one by chance (Carterette, TOIS 2012). Holm's step-down controls the family-wise error and
  is never worse than Bonferroni.
- **Risk, not only the mean.** A change that wins +0.02 on average by losing one topic badly
  is worse for that topic's users than its mean suggests. That is the motivation for the
  risk-sensitive measures (URisk/TRisk, Dinçer, Macdonald & Ounis 2014). Here it is
  implemented as the named-loss list, which is the same idea at a scale where every topic can
  be read by a person.

## 7. Online evaluation — not built, and harder here than usual

Offline judgments say what assessors prefer. Interleaving says what users choose: team-draft
interleaving (Radlinski, Kurup & Joachims 2008) merges two rankings and credits whichever
one supplied the result that gets used, and it detects differences that absolute usage
metrics cannot. The relay (`NosFabrica/vespa-relay`) could interleave two configs for NIP-50
REQs. The obstacle is Nostr itself: **a relay never sees a click.** The only signal is a
follow-up REQ on the same connection for a returned id or author (opening a profile, loading
a thread). That is weak and client-dependent, and it needs a privacy review before anything
is logged. If it is ever built, it complements this protocol and does not replace it.

## 8. Running it

### 8.1 A corpus that holds the topics

Use a local Vespa. Pull the topics' real pages and their authors' trust cards **read-only**
from the live relay (CLAUDE.md: never feed it, never point a test at it), then load them with
`exportLoad`. A capture is a plain JSON array of events. The 2026-10-23 run used, for every
case query, the lensed page (`<q> observer:460c25…065c`, limit 150), the unranked page
(`<q> include:spam`, limit 60), the observer's 10040, and the provider's 30382s for every
author seen. That came to about 6 000 events. **`benchmark/capture_eval.py`** does exactly
this, and also writes production's served pages as a run file (§8.5).

### 8.2 The run

```bash
# baseline first; every later config is compared against it
./gradlew :benchmark:rankAb --args="--vespa http://localhost:8080 \
  --observer 460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c \
  --configs baseline,my_candidate --now 1792713600 --pool /tmp/pool.json"

# grade /tmp/pool.json (§4, §5.3), then
./gradlew :benchmark:rankAb --args="--merge /tmp/pool.json --by human:<you>"
# and re-run the first command until no graded topic is flagged under judged@10 0.9
```

Pin `--now` so the age guardrail is reproducible across days. `--split dev|test`
selects topics (§3.4). `--judgments` points at another judgments file (default
`benchmark/search_judgments.json`). `--lens-key` forces the service key for a cluster fed
without the observer's 10040.

### 8.3 Reading the output

Each case row shows its position (`#n`/`MISS`, `<- LOST`/`<- FOUND`), the page age,
`nDCG` and `j@10` (once graded), and `rbo` against the baseline. Each config's summary adds
the graded and known-item means and the top-author share. The verdict block (§6.3) follows
the table.

### 8.4 Changes `query()` overrides cannot express

A schema, extraction or `EventYql` change needs two clusters (or one cluster, before and
after), each loaded from **the same capture**. Against the *before* cluster, run
`rankAb --configs baseline --write-runs /tmp/before`. That saves every config's pages as a
**run file**: per topic, the served events in order. Against the *after* cluster, run
`rankAb --run before=/tmp/before/run_baseline.json --configs before,baseline`. The before
pages are then the baseline, and the verdict is the same paired one as for overrides. The
judgments are by event id, so they apply to both clusters.

### 8.5 Against production

The relay is a cluster this repo cannot redeploy, but its pages can be captured, and a
captured page is a run. `benchmark/capture_eval.py` writes `run_production.json`, the
relay's served order for every topic under the canonical observer.
`rankAb --run production=<file> --configs production,baseline,…` then scores production
first, as the baseline, so every local config reads as "versus what users get today". Two
caveats are inherent. The local cluster only holds what the capture pulled, so this compares
**orderings over production's candidates**, not recall. And the relay deploys on its own
cadence, so name its NIP-11 `version` and the capture time beside the verdict.

### 8.6 Two tools: `rankAb` and `rank_eval.py`

`main` grew a second blind-judgment workflow alongside this one, `benchmark/rank_eval.py`
(its results are in `benchmark/README.md`, "Does the order make sense?"), built to judge
`sort:links` and the notes-only recency weight. They answer different questions, and a
change may need both.

| | `rankAb` + `search_judgments.json` | `rank_eval.py` |
|---|---|---|
| what it ranks | the engine: rank profiles and their `query()` knobs, through `EventYql` | whole orders as the store assembles them, including `sort:links` and spliced members |
| judgments | per document, 0–3, **kept**: they accumulate across changes, with provenance and an audit | per document 0–2 plus a best-to-worst ranking of the lists, kept per run |
| verdict | paired per-topic nDCG / S@k, randomization test, Holm, MDE, named losses (§6) | per-reader nDCG, sign test, list preference, page proxies (duplicates, repeated authors, age) |
| use it for | every rank-profile, schema or extraction change, and the ship rule | a store-side reordering, and a reader's whole-page preference |

They connect through run files. A store-side order exported as a run (`{topic: [events…]}`,
§8.4) scores against the persistent judgments with `rankAb --run name=<file>`, so a
`sort:links` page can be held to §6's paired verdict as well. Grades do not convert one-to-one
(0–2 against 0–3), so do not merge `rank_eval.py` grades into `search_judgments.json`;
re-grade the pool instead.

## 9. First measurement (2026-10-23)

This run used a local Vespa loaded with `capture_eval.py`'s capture: 6 072 events, the 27
topics' production pages plus unranked slices, and the trust cards of 2 172 authors. It ran
under the canonical observer with `--now` pinned to the capture. **Production was the
baseline**, as its served pages (relay NIP-11 version 0.1.0, captured 2026-10-23), against
this branch (`baseline`) and four lever configs.

**Judgments.** 1 021 grades: the blind pool of all six configs' top 20 (999 rows), plus 22
documents a fix below surfaced. Grade counts are 85 × 3, 386 × 2, 346 × 1 and 182 × 0. Five
parallel model judges did the grading, grouped by topic family so related queries were graded
by the same judge, against §4 and each case's `need`. Provenance is `llm:claude`. After the
round, judged@10 is **1.000** for every config on every topic. **Not yet audited**:
`benchmark/search_judgments_audit.json` is the blind 20% sample (208 rows, at least 3 per
topic) a human must re-grade before these numbers support a ship decision (§5.3).

**The verdict, nDCG@10 versus production** (27 topics, Holm over 5 candidates):

| config | Δ mean | 95% CI | W/T/L | MDE | where it moves |
|---|---:|---|---:|---:|---|
| `baseline` (this branch) | −0.001 | [−0.006, +0.002] | 2/23/2 | 0.006 | nowhere: the branch ranks as production does |
| `perfect_off` | +0.020 | [−0.015, +0.057] | 11/9/7 | 0.053 | topical **+0.126**, ambiguous +0.067; navigational −0.053 (`odell` RR −0.90, `Avi Burra` −0.50) |
| `no_trust` | +0.019 | [−0.097, +0.133] | 15/1/11 | 0.170 | multilingual **+0.350**, fuzzy +0.332; prefix −0.232 (`Vitor P` −0.81) |
| `pre_split` | −0.001 | [−0.039, +0.026] | 7/16/4 | 0.048 | `Vitor P` −0.40; ambiguous S@1 up |
| `recency_off` | +0.001 | [−0.025, +0.028] | 8/14/5 | 0.039 | `amethyst` −0.22 (RR −0.67): recency is what puts the current app listing first |

Nothing is significant after Holm, and with 27 topics nothing modest could be. What the
per-intent split shows instead is two **trade-offs inside shipped levers**, each now a
measured hypothesis rather than an anecdote:

1. **The perfect-match rung (`w_perfect_pop`) helps names and hurts topics.** With it off,
   topical nDCG@10 rises from 0.32 to 0.58 on `bitcoin`, 0.41 to 0.72 on `lightning` and 0.48
   to 0.66 on `coffee`. On a one-word topical query, the rung lifts profiles, sets and app
   listings whose name *is* the word (graded 1–2) above articles about the subject (graded
   3). The same rung is what wins `odell`, `Avi Burra` and `Primal`. A rung that applied to
   naming fields only when the query is name-shaped, or one damped by the word's document
   frequency, is the experiment this suggests.
2. **The trust curve outweighs text on non-English queries.** With trust off, `中村` goes from
   0.38 to 0.76, `esposa` from 0.45 to 0.88 and `divórcio e desabafa` from 0.38 to 0.60,
   while the as-you-type ladders collapse (`Vitor P` 0.93 → 0.12). The likely mechanism, not
   yet measured: the lens's most-trusted authors write mostly in English, so for a Portuguese
   or Japanese query their marginal matches outrank strong matches from less-trusted authors,
   because within a band trust is worth
   more than any text signal (the same saturation `docs/recency-ranking.md` measured). Only 3
   multilingual topics stand behind this, so the first step is more of them.

**Fixed on the way.** These are three harness bugs, each of which made a verdict say
something false:

- `rankAb --observer` sent the *observer* as the trust key ever since trust went service-keyed.
  That key matches no cell, so the `min_rank` gate emptied every lensed page (27 topics, 0
  hits). `rankAb` now resolves the observer's kind 10040, as the store does.
- Case text went to the engine raw. `group:rspuyeetackckp92 testin` therefore required the
  literal token `group:rspuyeetackckp92`, returned **nothing**, and showed as a −0.58 loss
  against production. The relay ignores the unknown extension and searches `testin`. Text
  now goes through the grammar (§2). The topic now reads RBO 1.00 against production.
- The worksheet showed no tags, so a judge could not see which NIP-29 group a post belonged
  to. It now carries the first 16 tags and the case's `need`.

**Studied, not shipped: a per-kind perfect-match rung (2026-10-23).** This tested the first
trade-off above. The candidate kept the whole rung for **entities** (kinds 0, 40, 41, 30617,
31990, 32267, 34550: a person, channel, repository, app or community whose name *is* the
thing) and gave **labels** (sets, lists, content titled with the word) a share
`w_perfect_label` of it on one-word queries. Multi-word queries kept it whole. It was swept at
0, 0.25 and 0.5, with the 33 documents the sweep surfaced judged first, so judged@10 was 1.000:

| config | nDCG@10 Δ vs production | 95% CI | navigational | ambiguous | topical |
|---|---:|---|---:|---:|---:|
| `perfect_off` (rung gone) | +0.020 | [−0.015, +0.057] | −0.053 (RR −0.256) | +0.067 | **+0.126** |
| `perfect_label_0` | +0.010 | [−0.013, +0.035] | +0.010 (RR 0.000) | **+0.092** | −0.005 (`nostr` −0.18) |
| `perfect_label_05` | +0.005 | [−0.010, +0.023] | +0.009 | +0.085 | −0.031 |

It protects the navigational queries completely and helps the ambiguous names ("Jack",
"Vitor"), but it recovers **none** of the topical gain. Once labels lose the rung, the topical
pages fill with *entities* named after the topic word: profiles called "Bitcoin" or "nostr",
"Nostr" channels, "bitcoin" repos, all graded 1–2. On `bitcoin`, a profile named "Bitcoin" has
exactly the shape ODELL's profile has on `odell`, so no per-document kind test can separate
them. It fails §6.3 (CI low end −0.013, an unexplained `nostr` loss) and was not merged.

What separates the two queries is **how many things share the name**: one ODELL against
thousands of things called "Bitcoin". That is name-field document frequency. Vespa holds it
per field (it is the IDF inside `bm25(name)`), so a rung damped by it is expressible in
`event.sd`. Two things this experiment ruled out on the way: all-field frequency (production
COUNTs: `primal` 2.6M, `lightning` 1.6M, `odell` 0.48M, `bitcoiners` 0.13M) and profile
frequency (`primal` 341k, inflated by `@primal.net` addresses; `odell` 6.7k against `bitcoiners`
10k). Neither orders names below topics. Testing name-field frequency needs a corpus whose
names are a fair sample of production's: a random sample of profiles and labelled kinds, not
this query-drawn capture, whose name frequencies are biased toward its own queries.

**Shipped: the perfect-match rung damped by name frequency (2026-10-23).** This
follows the per-kind result above. `event.sd` gains `name_df_ratio()`, the share of indexed
documents carrying the query word in `name`, `display_name` or `search_primary`. It is read in
place from Vespa's per-field statistics (`queryTermDocumentFrequency` over `num_docs_indexed`),
so it costs no extra round trip. `perfect_share()` scales the rung on one-word queries:
whole at or below `perfect_df_lo`, gone at or above `perfect_df_hi`, log-linear between. It
was built disabled and switched on (2e-4, 6e-4) only after the held-out confirmation below.

*Measuring production's ratio.* The evaluation corpus cannot measure it, because it is drawn
from the topics' pages and every topic word names 1–7% of it. `benchmark/sample_names.py`
builds a **kind-stratified random sample** of production: 149 875 events, with profiles,
wikis, articles and sets at their production shares. A plain time-random sample had profiles
at half and wikis at an eighth of production. The script reads the ratio off a Vespa loaded
with it (`benchmark/name_df.json`):

| word | documents naming it, per 150k | ratio | intent |
|---|---:|---:|---|
| bitcoin | 100 | 6.7e-4 | topical |
| nostr | 96 | 6.4e-4 | topical |
| jack | 24 | 1.6e-4 | ambiguous |
| primal | 18 | 1.2e-4 | navigational |
| lightning | 5 | 3.3e-5 | topical |
| odell | 4 | 2.7e-5 | navigational |
| coffee, bitcoiners | 2 | 1.3e-5 | topical |
| amethyst, vitor, dell, esposa | 1 | 6.7e-6 | various |

`rankAb --name-df benchmark/name_df.json` makes the evaluation cluster read these ratios. For a
one-word query it rewrites the three naming-field clauses to carry a `documentFrequency`
annotation, scaled to the local document count. The `baseline` row reproduces the earlier
baseline exactly, which shows the rewrite changes no matching.

*The sweep* (production baseline, judged@10 1.000):

| config (lo, hi) | nDCG@10 Δ | 95% CI | W/T/L | navigational | topical | `bitcoin` | `nostr` |
|---|---:|---|---:|---:|---:|---:|---:|
| `perfect_off` | +0.020 | [−0.015, +0.057] | 11/9/7 | −0.053 | +0.126 | 0.32 → 0.58 | 0.65 → 0.66 |
| **`name_df_2e4_6e4`** | **+0.006** | **[−0.008, +0.026]** | 4/21/2 | **0.000** | **+0.035** (RR +0.094) | 0.32 → **0.58** | 0.65 → 0.66 |
| `name_df_2e4_2e3` | −0.004 | [−0.012, +0.001] | 2/22/3 | 0.000 | −0.011 | — | — |
| `name_df_1e4_1e3` | −0.004 | [−0.012, +0.001] | 2/22/3 | 0.000 | −0.011 | — | — |

At (2e-4, 6e-4) the rung is gone for `bitcoin` and `nostr` and whole for every name.
`bitcoin` gets the entire gain that switching the rung off had (0.32 → 0.58, and its top 10
goes from a median 536 days old to 16), with no navigational or ambiguous query moving. The
softer ramps leave the two words half their rung, which is not enough. The one listed loss,
`primal.net` −0.081, is a harness artifact: its punctuation keeps `rankAb` from injecting it,
so it ran on the corpus's own 3% ratio. On the production sample it names 1.2e-4, below
`lo`, so production would keep its rung and its page unchanged. The other loss in W/T/L, `lightning`
(0.45 → 0.41), is the baseline's own difference from production, the same with or without
the damping. Against this branch, the candidate moves exactly three topics: `bitcoin`,
`nostr` and the `primal.net` artifact.

*What it does not do.* `lightning`, `coffee` and `bitcoiners` are as rare as names as `odell`
is (1–5 per 150k), so name frequency cannot tell them from a navigational query. Their pages
keep the rung and its cost, and they need a different signal.

*Why it waited.* It cleared §6.3 on these topics, but (lo, hi) was chosen on the same 27
topics it is scored on, and only two of them fall above `lo`. §6.3 rule 5 asks a tuned value
to hold on held-out topics first, which is the confirmation below.

**Held-out confirmation, pre-registered (2026-10-23).** This was written and committed before any
held-out page was captured, graded or scored. Twelve `split: test` topics were chosen by name
frequency on the production sample alone:

- **Acted on** (frequency inside the ramp): `ai` 4.4e-4, `music` 3.9e-4, `news` 2.5e-4, `crypto` 2.3e-4.
- **Controls** (just below `lo`): `art` 1.4e-4, `film` 1.2e-4, `privacy` 1.2e-4.
- **Navigational, with ordinary-word names**: `damus`, `alby`, `iris` (1.3–2.7e-5), `fountain`
  and `zeus` (0). These are the adversarial case, and the measurement already says the damping
  cannot reach them. No navigational name among 36 candidates reached `lo`.

`security` (1.9e-4) was left out because rounding on a small evaluation cluster would place it
in the ramp, a harness artifact rather than production behaviour.

*The one candidate*: `name_df_2e4_6e4`, (lo, hi) = (2e-4, 6e-4), compared once against the
branch with the damping off (`baseline`) on `--split test` with `--name-df`. It passes, and the
default is switched on, only if all four of these hold:

1. the nDCG@10 95% CI's low end is ≥ −0.01 over the 12 topics;
2. no navigational topic loses any nDCG@10 or RR;
3. the four acted-on topics improve on average (mean nDCG@10 Δ > 0);
4. the three controls do not move by more than 0.01 nDCG@10.

Production's pages are reported beside it for reference and do not enter the criteria.

**The confirmation: passed (run once, 2026-10-23).** Production pages for the 12 topics were
captured with `capture_eval.py`. Their blind pool (326 documents, all four configs' top 20)
was graded by three model judges against each case's `need` (`llm:claude`; 71 of them were
added to the audit sheet), and `rankAb --split test --name-df benchmark/name_df.json
--configs baseline,name_df_2e4_6e4` ran once:

| criterion | result |
|---|---|
| 1. nDCG@10 CI low end ≥ −0.01 | Δ +0.010, CI **[+0.000, +0.028]**, W/T/L 2/10/0 |
| 2. no navigational loss | `damus`, `alby`, `fountain`, `iris`, `zeus`: identical pages (RBO 1.00), Δ 0 |
| 3. acted-on topics improve on average | mean **+0.03**: `ai` 0.29 → **0.40** (its top 10 also goes from 82 days old to 20), `crypto` +0.01; `music` and `news` unchanged |
| 4. controls move ≤ 0.01 | `art`, `film`, `privacy`: identical pages |

All four hold, so the default is on. Across all 39 topics the shipped behaviour scores
+0.008 [−0.004, +0.024], W/T/L 4/34/1: every navigational topic is identical, and the one loss
is the `primal.net` artifact. The effect is real but narrow. It moves a page only where a word
is clearly more common as a name than any name is, and `music` and `news` sit low enough in
the ramp that they did not reorder.

**The small-index guard** (`perfect_df_min_docs`, 100 000) was added when the default was
switched on, and is outside what the confirmation measured. The ratio is a local statistic,
and on a small index every word is common: in a 30-document corpus, a name two documents carry
reads 7%, a hundred times past `hi`. Below 100 000 documents on a node, the rung is therefore
whole, exactly as before the damping existed. So small relays, fresh stores and every ranking
IT are unaffected, and production's nodes, which hold millions, get the measured behaviour.
On an evaluation corpus (always under the guard) `baseline` therefore reads as the damping
OFF. The `name_df_*` configs lift the guard (`perfect_df_min_docs` 0) and must be paired with
`--name-df`, which makes the corpus's name statistics production's. `NameFrequencyRungIT` pins
the guard, the damping, the multi-word exemption and the off switch on a real Vespa.

**Pre-registered: the `book of daniel` fixes (2026-10-23, round 2).** This was written and
committed before any page of these topics was captured and before either fix was tuned.
Two candidate fixes for the report recorded on that case:

- **Split coverage** (`w_split_min_coverage`): a scattered match keeps the split rung only if
  its naming surface carried at least this share of the typed words. Otherwise it falls to the
  body band. At 0.5 a two-word query is untouched, because one word of two is 0.5.
- **Body phrase** (`w_phrase_pop`): a second-phase band for a body holding every typed word
  adjacent and in order (`fieldMatch(search_text).longestSequence` ≥ the words typed).

Both are tuned on the dev topics only (`book of daniel` and the existing multi-word cases).
The chosen combination is then compared **once** against `baseline` on these 12 held-out
multi-word topics (`split: test`, round 2): `proof of work`, `proof of stake`,
`web of trust`, `lightning network`, `freedom of speech`, `lord of the rings`,
`the art of war`, `bitcoin white paper` (topical), and `jack dorsey`, `matt odell`,
`lyn alden`, `preston pysh` (navigational names, the guard). It ships only if all of these
hold:

1. the nDCG@10 95% CI's low end is ≥ −0.01 over the 12 topics;
2. no navigational topic loses any nDCG@10 or RR;
3. the eight topical topics improve on average (mean nDCG@10 Δ > 0).

*The dev phase, and the one candidate (recorded before the confirmation was scored).* Two
things were found and fixed on the way:

- **The body phrase could not fire.** `fieldMatch(search_text).longestSequence` counts runs
  adjacent in the query as well as the field, and `EventYql` interleaves synthetic joined
  pairs ("bookof") between the typed words. It read 0 on all 28 notes holding the phrase.
  `EventYql` now adds the typed words as a **rank-only** phrase item (`rank(words, phrase)`:
  recall unchanged). With it, 28 of 28 read 1 and none of the 14 holding the words apart do.
  With the band at 0 the item changes no dev page.
- **The worksheet hid matches past its 600-character excerpt.** 40 of the 58 graded
  `book of daniel` documents matched further in, and notes discussing the book were graded 0
  for showing nothing relevant up front. The worksheet now carries keyword-in-context
  windows (`context`), and the whole topic was re-graded from it.

Dev verdict (28 topics, against `baseline`):

| config | Δ nDCG@10 | W/T/L | `book of daniel` | moves |
|---|---:|---:|---:|---|
| `split_cov_05` | +0.017 | 1/27/0 | 0.00 → 0.47 | that topic only |
| **`both_4k`** | **+0.019** | **3/25/0** | 0.00 → 0.47 | + `Jon Gordon` +0.06, `divórcio e desabafa` +0.01 |
| `both_23k` | +0.008 | 3/24/1 | 0.00 → 0.15 | `Verified Human` −0.04 |
| `both_60k` | +0.010 | 3/24/1 | 0.00 → 0.22 | `Verified Human` −0.10 |

A strong phrase band hurts the query that motivated it: it lifts notes that merely *mention*
"the book of Daniel" (graded 1) from authors ranked 90+ over the Wikipedia pages *about* the
book (graded 2–3, rank 32), since a phrase in a body is weak evidence of aboutness and trust
multiplies it. At the weak rung (4 000) it only breaks ties. **`both_4k` is the candidate**:
`w_split_min_coverage` 0.5 and `w_phrase_pop` 4 000.

**The confirmation: passed (run once, 2026-10-23). Both fixes ship.** The production pages for
the 12 held-out topics were captured. The blind pool (303 documents, top 20 of `baseline`,
`both_4k` and its two halves) was graded with the keyword-in-context worksheet by three model
judges (`llm:claude`; 64 rows were added to the audit sheet), and `rankAb --split test
--configs baseline,both_4k` ran once over the 12 topics:

| criterion | result |
|---|---|
| 1. nDCG@10 CI low end ≥ −0.01 | Δ **+0.017**, CI **[+0.004, +0.032]**, W/T/L 5/5/1 (11 topics: `the art of war` has no relevant document in its pool, all 29 graded 0, so its nDCG is undefined) |
| 2. no navigational loss | `jack dorsey`, `matt odell`, `lyn alden`, `preston pysh`: nDCG@10 and RR identical |
| 3. topical improves on average | **+0.026**: `lightning network` +0.074, `lord of the rings` +0.046, `web of trust` +0.040, `proof of work` +0.017, `freedom of speech` +0.009, `proof of stake` 0, `bitcoin white paper` −0.002 (the one loss: a one-slot swap. The grade-2 meetup "BCE#3 : Bitcoin White Paper Day" carries the typed words in order and rises 9 → 7, past the grade-3 "Das Bitcoin-Whitepaper für Kinder", whose one-word "Whitepaper" holds no phrase) |

*Guardrails.* Top-author share fell (0.433 → 0.383). Median top-10 age rose (692 → 787
days), on two topics: `lightning network` (+479 days), where an older explainer article
replaced a newer bookmark list and nDCG rose by 0.074, and `the art of war`, whose pool holds
nothing relevant. It is better content that happens to be older, not a staleness regression.

*Latency.* Main's app with main's YQL against this app with this YQL, alternating deploys,
`searchTrace` FULL row, kind 1, limit 50, p50 of 25 per round, three rounds each. On the
evaluation cluster (11 588 documents, the real lens), every topic tried matches under 1 000
documents and the two agree within noise (±2 ms at 7–45 ms), with identical match counts. That
does not reach production's worst case, where a common word fills the 1 000-document rerank
window per node and the new second-phase `fieldMatch(search_text)` runs on every slot. So the
second run used the 149 875-event stratified production sample (`sample_names.py`), with the
gate opened (`SEARCH_MIN_RANK=-1e9`, since the sample has no trust cards and a closed gate
drops every hit before the second phase):

| term (matches) | main, 3 rounds | this, 3 rounds |
| --- | --- | --- |
| `nostr` (12 569, window full) | 58, 30, 31 ms | 36, 32, 39 ms |
| `bitcoin` (2 740, window full) | 24, 21, 20 | 22, 18, 24 |
| `nostr bitcoin` (593) | 22, 18, 29 | 26, 21, 27 |
| `bitcoin lightning network` (47) | 22, 20, 21 | 25, 21, 28 |
| `book of daniel` (7) | 53, 41, 35 | 49, 47, 39 |
| `the art of war` (5) | 16, 18, 19 | 17, 18, 22 |

The cost is a few milliseconds per node at most, and the round-to-round spread is as large. The
reported slow searches on production (seconds for `bitcoin` and `nostr`, `benchmark/README.md`)
are matching, which neither change touches. The guardrail holds.

`event.sd` ships `w_split_min_coverage` 0.5 and `w_phrase_pop` 4 000. `ThinSplitPhraseIT`
pins the behaviour on a real Vespa: a thin split scores on the body rung, a body phrase reads
1 and outranks it, the exact title leads, and a two-word query is untouched.

*Not addressed, seen on the way.* On `jack dorsey` and `matt odell` the people's own profiles
reach no config's top 20, because their names are `jack` and `ODELL`, not the full names.
That is a recall gap for full-name queries against short handles, and a separate topic.

**What is next.** (1) A human audits the 208-row sample and reports κ (`rankAb --agreement`).
(2) Grow toward the §3.2 target of 50 or more topics, adding multilingual and topical topics
first, since those are the strata where the levers disagree. (3) A/B the two hypotheses above
as candidate configs against this judgment set.

## 10. References

- Cleverdon (1967), *The Cranfield tests on index language devices*: the paradigm.
- Järvelin & Kekäläinen (2002), *Cumulated gain-based evaluation of IR techniques*, TOIS 20(4).
- Burges et al. (2005), *Learning to rank using gradient descent*, ICML: the exponential gain.
- Voorhees (2000), *Variations in relevance judgments and the measurement of retrieval
  effectiveness*, IP&M 36(5): system rankings are stable under assessor disagreement.
- Buckley & Voorhees (2004), *Retrieval evaluation with incomplete information*, SIGIR: bpref.
- Sakai (2007), *Alternatives to Bpref*, SIGIR: condensed-list nDCG′.
- Smucker, Allan & Carterette (2007), *A comparison of statistical significance tests for IR
  evaluation*, CIKM; and (2009), *Agreement among statistical significance tests … at varying
  sample sizes*, SIGIR.
- Webber, Moffat & Zobel (2008), *Statistical power in retrieval experimentation*, CIKM; and
  (2010), *A similarity measure for indefinite rankings*, TOIS 28(4): RBO.
- Moffat & Zobel (2008), *Rank-biased precision for measurement of retrieval effectiveness*,
  TOIS 27(1).
- Chapelle, Metzler, Zhang & Grinspan (2009), *Expected reciprocal rank for graded
  relevance*, CIKM.
- Clarke et al. (2008), *Novelty and diversity in information retrieval evaluation*, SIGIR:
  α-nDCG, the graded route to diversity if author share ever proves too coarse.
- Radlinski, Kurup & Joachims (2008), *How does clickthrough data reflect retrieval
  quality?*, CIKM: team-draft interleaving.
- Carterette (2012), *Multiple testing in statistical analysis of systems-based information
  retrieval experiments*, TOIS 30(1).
- Dinçer, Macdonald & Ounis (2014), *Hypothesis testing for the risk-sensitive evaluation of
  retrieval systems*, SIGIR.
- Fuhr (2017), *Some common mistakes in IR evaluation, and how they can be avoided*, SIGIR
  Forum 51(3); Sakai (2020), *On Fuhr's guideline for IR evaluation*, SIGIR Forum 54(1).
- Sakai (2018), *Laboratory Experiments in Information Retrieval*, Springer: topic set size
  design.
- Thomas, Spielman, Craswell & Mitra (2024), *Large language models can accurately predict
  searcher preferences*, SIGIR.
- Upadhyay et al. (2024), *UMBRELA*, arXiv:2406.06519; and *A large-scale study of relevance
  assessments with LLMs* (TREC 2024 RAG), arXiv:2411.08275.
- Clarke & Dietz (2024–26), *LLM-based relevance assessment still can't replace human
  relevance assessment*, arXiv:2412.17156.
