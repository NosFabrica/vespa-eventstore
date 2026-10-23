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
instead (§8.4). One gap is inherited from `rankAb`: case text goes to `EventQuery.search`
RAW, below the store's syntax parser. A topic using `-word`, `"phrases"` or `sort:` is
measured as loose words, so it evaluates the engine and not the grammar. Such a topic has to
go through the store's `FilterMapping` before it can be graded meaningfully.

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
author seen. That came to 6 076 events. `benchmark/capture_staging.py` shows the REQ shapes.

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
after), each loaded from **the same capture**. Run `rankAb --configs baseline` against each,
keep the output, and compare the two runs' per-topic rows. The judgments are by event id, so
they apply to both. A paired comparison tool across two runs does not exist yet. Until it
does, compare per-topic rows by hand and state n, W/T/L and the losses in the PR.

## 9. First measurement (2026-10-23)

This was a local Vespa loaded with the §8.1 capture, under the canonical observer. It is
recorded to show the machinery working and the limits it exposes, not as a quality baseline.

- **Fixed on the way**: `rankAb --observer` had been sending the *observer* as the trust key
  ever since trust went service-keyed. That matches no cell, so the `min_rank` gate emptied
  every lensed page: 27 topics, 0 hits. `rankAb` now resolves the 10040 like the store does.
- **Known-item (8 topics, RR)**: `perfect_off` Δ −0.079 [−0.204, 0.000], p 0.5 (only two topics
  moved, so half of all sign assignments are as extreme; 1.0 after Holm over two candidates), W/T/L
  0/6/2, MDE 0.175. Losses: `Avi Burra` −0.50 (#1→#2) and `Primal` −0.13 (#3→#5).
  `no_trust` Δ −0.021, MDE 0.058. The losses are real and visible, and **eight topics cannot
  establish them**. That is §3.2 in one line.
- **Pool**: 476 unjudged documents in the union of three configs' top 10 across 27 topics,
  about 18 per topic. That is the first judging round.
- **Grading two topics' pools** (`amethyst`, `Avi Burra`; a demonstration only, not
  committed) moved `amethyst` from an uncountable MISS on its stale `expect` (§5.4) to
  nDCG@10 0.83, and showed `no_trust` losing on both graded topics. That is the reading
  `rankAb` could not produce before.

`benchmark/search_judgments.json` does not exist yet. Creating it, by judging that first
pool with provenance, is the next step.

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
