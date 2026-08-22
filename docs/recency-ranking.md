# Recency in text ranking — a proposal

**Status: proposal. Nothing here is implemented.** Written 2026-08-22 against
`engine/app/schemas/event.sd` at `fb0eaa1`. It ends with the exact diffs, a
calibration derivation, and the measurement that has to pass before any of it
is turned on.

## 1. The gap

`created_at` orders every *plain* recall in this store and is invisible in every
*text* one:

| profile | what orders hits | recency? |
| --- | --- | --- |
| `unranked` / `recency` | `order by created_at desc` | it **is** the order |
| `recency_gated` / `recency_gated_exact` | `first-phase: attribute(created_at)` | it **is** the score |
| `search` (default, observer resolved) | `floored_text_score() * wot_mult()` | **none** |
| `text` / `text2` (observer-less) | `relevance()` — the additive tier ladder | **none** |
| `rank_desc` / `rank_asc` / `sort_followers` | tier ladder + trust / follower key | **none**, by request |

So a NIP-50 search for `bitcoin` scores a 2015 note exactly like this morning's
one from the same author. The only way a caller gets recency today is
`sort:recent`, which throws the *whole* relevance computation away — the store
even drops the text weights client-side for that shape (`EventYql.build`,
`TEXT_RANK_FEATURES`). Users asking for "newer posts first" are not asking for
that trade; they want the same ranking, tilted.

## 2. What the shape of the change is constrained by

Seven things in this repo decide what a recency signal is allowed to look like.
They are the reason this is a page of design and not a one-line `+ attribute(created_at)`.

1. **The `search` bands are multiplicative and calibrated.** `wot_mult()` is
   convex (`1 + w_wot · delta^2.7`) specifically so that trust crosses a text
   tier only on an overwhelming advantage (~7.6× delta for the bio→name gap),
   pinned from both sides by the 2026-08-02 "odell" case. The equal-trust rung
   floors are ~680 / 4 000 / 23 000 / 130 100 — each rung ~×5.65–5.88 the one
   below. **Any recency factor bounded under 5.65 provably cannot cross a
   rung**, whatever the corpus. That bound is the safety argument, and it only
   exists for a *multiplicative* term.
2. **`search` deletes hits by score.** `rank-score-drop-limit: 0.5`, and
   `floored_text_score()` maps gram-only noise to `0.0` for the limit to
   delete. A recency term that can *lower* a score changes the **match set**,
   not just the order — a recall regression invisible to anyone reading the
   first page. A factor `≥ 1` cannot: `x·m ≥ x` keeps every survivor, and
   `0·m = 0` keeps every drop. Recall stays bit-identical.
3. **`text`/`text2` bands are additive and soft.** `relevance()` rungs are
   1100 / 700 / 620 / 550 — ratios of 1.13–1.57 — and the within-band
   `bm25`/`secondary` tail is uncapped and already overlaps them. There is no
   clean "cannot cross" bound to buy there at any useful strength. Different
   profile, different weight, different appetite (§3.4).
4. **The corpus lies about time.** search-staging holds notes dated **2100**;
   they already top an unranked feed. `max(0, now − created_at)` — the shape of
   Vespa's own `age`/`freshness` features — hands exactly those documents the
   *maximum* freshness. Any recency function that is monotone in `created_at`
   is a spam gift.
5. **`created_at` is not "creation" for replaceable kinds.** Kind 0/3/10002 and
   the addressable kinds carry the timestamp of the **last edit**. Freshness on
   kind 0 rewards profile churn — and every calibrated case in
   `benchmark/rank_cases.json` is a kind-0 profile search.
6. **Ranking has to stay reproducible.** `RankRegressionIT` pins positions;
   `:benchmark:rankAb` compares configs across runs; multi-node clusters must
   score a document identically on every content node. Vespa's `now` rank
   feature is evaluated per node, per query, and drifts every second — a
   ranking that reads it is not a function of the request.
7. **No re-feed, no reindex, no new RAM.** `created_at` is already an
   in-memory `fast-search` attribute, already read in first-phase ranking of
   the recency profiles, and `docs/attribute-memory.md` marks it **never page**.
   A recency signal built from it costs one attribute read and no bytes.

## 3. The proposal

### 3.1 Two functions in `text_relevance`

```
event_age_days() = |clock − attribute(created_at)| / 86400
freshness()      = 1 / (1 + event_age_days() / query(recency_halflife))
recency_mult()   = 1 + w · freshness()          // w per kind, see §3.4
```

**Symmetric age** (`abs`, not `max(0, …)`) is constraint 4: a note dated 2100 is
as stale as one from 1952, which is the honest reading of "this clock is
wrong", while a client a few minutes ahead still counts as fresh. It is also
why the built-in `freshness(created_at)` is not used — that, and its shape:
built-in freshness is *linear* and hard-zeros past `maxAge`.

**Hyperbolic, not exponential.** `1/(1+age/H)` is 1 at zero age, 0.5 at the
half-life, and never reaches 0 — a three-year-old exact match is still the best
answer to a rare query, and `exp(−age)` has effectively deleted it by then.
It is also one divide, no `exp`/`log`, in a first phase that runs over every match.

| age | `freshness()` @ H=30d | `recency_mult()` @ w=1 |
| --- | --- | --- |
| today | 1.000 | 2.00 |
| 1 day | 0.968 | 1.97 |
| 1 week | 0.811 | 1.81 |
| 1 month | 0.500 | 1.50 |
| 3 months | 0.250 | 1.25 |
| 1 year | 0.076 | 1.08 |
| 5 years | 0.016 | 1.02 |

### 3.2 Where it multiplies

```
search.first-phase : floored_text_score() * wot_mult() * recency_mult()
search.second-phase: firstPhase + precision_boost_pop() * wot_mult() * recency_mult()
```

**First phase, not second.** The second phase only sees `rerank-count` (1000)
hits per node, chosen by the first phase — a common term matches ~50 000
(`docs/two-phase-ranking.html`). Recency confined to the second phase can only
reshuffle a window that recency had no say in selecting; the fresh note that
lost the first-phase cut is still gone. The cost of paying for it in phase 1 is
one attribute read and ~5 flops per match (§6).

**The second-phase boost is scaled too**, for the same reason its comment
already gives for `wot_mult()`: the phase-1 bands are multiplied by
`recency_mult()`, so a raw additive precision boost would be worth
`w/recency_mult()` band-points and the words/exactness rules would mean
something different for a fresh document than an old one. Multiplying keeps the
boost-to-band ratio invariant, which is what "reorders within a band" means.

### 3.3 Why `≥ 1`, and what it cannot do

`recency_mult() ∈ [1, 1+w]`. Two properties fall out, and both are the point:

- **Recall is unchanged** (constraint 2). No hit is dropped or resurrected —
  `VespaParityIT`'s 127 checks and `ObserverGateIT`'s gate assertions cannot
  move. Only order does.
- **Text tiers hold** (constraint 1). Crossing the smallest rung ratio needs
  `1 + w > 5.65`, i.e. **`w > 4.65`**. That is the hard ceiling; the practical
  one is lower, because rungs have tails (a bio-band doc carrying the term in
  body and hashtags reaches ~900 against the weak floor of 4 000 — 4.4×).
  **Recommended ceiling `w ≤ 2.0` (3×)**, which keeps ≥1.5× of margin to the
  nearest realistic band edge, and a shipped default to be picked by the A/B.

Note what this deliberately gives up: recency can **never** float a body
mention above a real name match, no matter how fresh. That is not a limitation
to fix later — it is the same rule that keeps trust honest.

### 3.4 One weight per kind class

```
w = if (attribute(kind) == 0, query(w_recency_profile), query(w_recency))
```

Constraint 5. A profile's `created_at` is its last edit; a freshness bonus
there is a bonus for editing your profile, which is free and which spam
accounts do. It is also the only place where the calibrated `rank_cases.json`
ladder lives. So kind 0 gets its own weight, defaulted to **0.0** — profile
search behaves exactly as it does today — and notes/articles/handlers get the
signal the request was actually about.

(The `RankRegressionIT` corpus is time-flat — every doc is `1_700_000_000 + n`,
seconds apart — so `freshness()` varies by <1e-7 across it and today's cases
stay green even with both weights on. The kind split is not there to keep the IT
green; it is there because the *live* kind-0 ladder is calibrated and the IT
would not notice if we broke it.)

### 3.5 The clock comes from the query

`query(now)`, stamped by `EventYql` from the client's clock, with
`if(query(now) > 0, query(now), now)` as the schema-side fallback. Constraint 6:

- **Deterministic tests.** `RankRegressionIT` and `RankAb` pin `now` to a fixed
  instant (the corpus's max `created_at`), so a recency case asserts a position
  that does not rot as the corpus ages.
- **Identical on every node.** All content nodes score one query against one
  instant.
- **Replayable.** "Why did this rank on Tuesday" is answerable by stamping
  Tuesday.

The store already stamps a clock into every query (`notExpiredAt`), so this is
the established shape, not a new dependency.

## 4. Calibration — what a freshness bonus is worth in trust

The only currency in `search` is the trust multiplier, so state the boost in
its units. With `wot_mult ≈ delta^2.7`, a multiplicative advantage `R` is worth
a trust-delta ratio of `R^(1/2.7)`. At **w = 1** (max 2×, H = 30d), the trust
advantage an *older* document needs to beat a *brand-new* one of equal text:

| the older doc is | needs this much more trust-delta |
| --- | --- |
| 1 day older | +0.6 % |
| 1 week | +3.7 % |
| 1 month | +11 % |
| 3 months | +19 % |
| 1 year | +26 % |
| ever older | +29 % (asymptote, `2^(1/2.7)`) |

Concretely, on the served 0–100 provider scale with `min_rank = 2`: today's
note from an author at trust 62 outranks last year's note from an author at
trust 77, and loses to one at 80. At **w = 2** the asymptote moves to +49 %
(trust 62 today beats trust 91 a year ago) — visibly a "news" ranking. At
**w = 0.5** it is +13 %, a tiebreak. Those three are the A/B ladder.

Half-life sets *where* the curve does its work, and should be chosen per
corpus: H = 7d for a conversation-shaped index, H = 30d for a general note
index (recommended starting point), H = 365d for long-form, where "recent"
means this year.

## 5. The diffs

### 5.1 `engine/app/schemas/event.sd` — `text_relevance` inputs

```
+           # ---- recency (docs/recency-ranking.md) ----
+           # The query instant, epoch seconds, stamped by EventYql. Vespa's
+           # own `now` is per-node and per-second, so a profile reading it is
+           # not a function of the request: tests could not pin a position,
+           # two content nodes could disagree about one query, and a report
+           # could not be replayed. Fallback below keeps an older client sane.
+           query(now) double: 0.0
+           # Age at which freshness() halves, in DAYS. Per corpus: ~7 for a
+           # conversation index, 30 for general notes, 365 for long-form.
+           query(recency_halflife) double: 30.0
+           # Strength of the multiplicative freshness boost: mult in [1, 1+w].
+           # 0.0 = OFF, the shipped default until the A/B says otherwise.
+           # HARD CEILING 4.65: above it the boost exceeds the smallest rung
+           # ratio of the §12 ladder (x5.65) and recency starts CROSSING text
+           # tiers. Recommended <= 2.0; rungs have tails (§3.3).
+           query(w_recency) double: 0.0
+           # Same, for kind 0. A replaceable event's created_at is its LAST
+           # EDIT, so freshness there rewards profile churn — and the whole
+           # calibrated rank_cases.json ladder is kind-0 search. Own weight,
+           # defaulted OFF; do not merge the two.
+           query(w_recency_profile) double: 0.0
```

### 5.2 `event.sd` — three functions in `text_relevance`

```
+       # Seconds between the event and the query instant, SYMMETRIC: a
+       # timestamp in the future is as stale as one equally far in the past.
+       # The live corpus really does hold notes dated 2100; max(0, now - t) —
+       # the shape of Vespa's own age()/freshness() — would hand exactly those
+       # the maximum boost, while abs() gives them a 74-year-old note's boost.
+       # A client a few minutes ahead of us still reads as fresh.
+       function event_age_days() {
+           expression: abs(if(query(now) > 0, query(now), now) - attribute(created_at)) / 86400.0
+       }
+
+       # 1 at zero age, 0.5 at the half-life, never 0. Hyperbolic, not
+       # exponential: a three-year-old exact match is still the best answer to
+       # a rare query, and exp() decay has deleted it by then. One divide, no
+       # exp/log — this runs over EVERY match, not a rerank window.
+       function freshness() {
+           expression: 1.0 / (1.0 + event_age_days() / max(1.0, query(recency_halflife)))
+       }
+
+       # >= 1, ALWAYS. That is what makes this recall-safe: `search` deletes on
+       # rank-score-drop-limit and floored_text_score() maps gram-only noise to
+       # 0.0 for it to delete, so a factor >= 1 can neither push a survivor
+       # under the limit nor lift a zeroed hit over it — the match set is
+       # bit-identical, only the ORDER moves. Bounded by 1+w, which is what
+       # keeps recency under the x5.65 rung ratio of the tier ladder.
+       function recency_mult() {
+           expression: 1.0 + if(attribute(kind) == 0, query(w_recency_profile), query(w_recency)) * freshness()
+       }
```

### 5.3 `event.sd` — the `search` profile

```
        first-phase {
-           expression: floored_text_score() * wot_mult()
+           expression: floored_text_score() * wot_mult() * recency_mult()
            rank-score-drop-limit: 0.5
        }
        second-phase {
-           expression: firstPhase + precision_boost_pop() * wot_mult()
+           # x recency_mult() for the same reason it is x wot_mult(): the
+           # phase-1 bands carry both multipliers, so a raw additive boost
+           # would be worth w/(wot_mult*recency_mult) band-points and the
+           # precision rules would mean something different for a fresh doc.
+           expression: firstPhase + precision_boost_pop() * wot_mult() * recency_mult()
            rerank-count: 1000
        }
        match-features {
+           freshness
+           recency_mult
            ...
        }
```

`match-features` is not optional here: since 2026-08-05 this store's answer to
"why is this above that" is the inspector's feature dump, and a ranking input
nobody can see is a ranking input nobody can debug.

### 5.4 `event.sd` — the text profiles (phase 2, weight 0 until asked for)

`text`/`text2` get `* (1.0 + query(w_recency_text) * freshness())` on
`relevance()`, with `query(w_recency_text) double: 0.0`. Kept separate and
kept off in phase 1 on purpose: those bands are additive, 1.13–1.57 apart, and
already overlapped by their own uncapped `bm25` tail (constraint 3), so there
is no band-safety proof to inherit — and `text` is only the observer-less
fallback. Tune it if and when someone runs observer-less search in anger.

### 5.5 `:engine` query side

```kotlin
// EventQuery
/**
 * The query instant for recency ranking (epoch seconds). Null = the wall
 * clock, which is what production sends. Harness knob, like [rerankCount]:
 * a fixed value makes a ranking assertion reproducible as the corpus ages.
 */
val nowSecs: Long? = null,

/**
 * Rank-feature overrides layered onto the request (`ranking.features.query(x)`).
 * Harness-only — RankAb and the rank ITs sweep w_recency / recency_halflife
 * without a redeploy. Production queries trust the profile's defaults.
 */
val rankFeatures: Map<String, Double> = emptyMap(),
```

`EventYql.build`, for the profiles that actually read it — `search`, `text`,
`text2`. Not `unranked`/`recency`/the two gated profiles (they carry no text
ranking at all), and not `rank_*`/`sort_followers` (explicit sorts, §9.3):

```kotlin
params["ranking.features.query(now)"] = (q.nowSecs ?: System.currentTimeMillis() / 1000).toString()
q.rankFeatures.forEach { (k, v) -> params["ranking.features.query($k)"] = v.toString() }
```

and `query(now)` joins `TEXT_RANK_FEATURES` so the gated profiles keep sending
nothing they do not declare. No `SchemaFallbacks` demotion is needed — a scalar
query feature a serving profile does not declare is ignored, not a 400 — but
**verify that against staging** before relying on it; it is the kind of claim
this repo has been burned by.

### 5.6 Harness

- `RankAb.CONFIGS` += `recency_off`, `recency_h30_w05`, `recency_h30_w1`,
  `recency_h30_w2`, `recency_h90_w1`, `recency_h365_w1` — pure `query()`
  overrides, so the whole sweep runs against a live cluster with **no deploy**
  once §5.1–5.3 are in the serving schema.
- `benchmark/rank_cases.json` += the recency-shaped reports. Every case there
  today is a profile/name shape; the ones this change is for are body-term
  searches on a corpus with 132M kind 1s (`bitcoin`, `nostr`, a breaking-news
  term). Capture them from staging **with** and **without** the lens, as the
  file's own note demands.
- `RankRegressionIT` += a time-spread fixture the current corpus deliberately
  lacks: one author, one text, `created_at` at `now−1d / now−1y / now−5y`, plus
  one dated 2100. Assert (a) with `w_recency = 0` the order is exactly today's,
  (b) with `w_recency = 1` it is newest-first, (c) the 2100 doc is never first,
  (d) a fresh bio mention still loses to an old exact name match — the tier
  ladder is not for sale.

## 6. Cost, and what has to be measured

Per matched document, first phase: one `attribute(created_at)` read (already
resident, already `fast-search`, already marked never-page in
`docs/attribute-memory.md`), one subtract, one `abs`, two divides, one
multiply-add, one `if` on `attribute(kind)`. No new field, no reindex, no
re-feed, no RAM. The change is a schema deploy plus the query-side stamp.

That is an argument, not a measurement. Before the weight leaves 0:

1. `./gradlew :benchmark:searchBench` A/B, `w_recency = 0` vs `1`, on the same
   cluster — the first phase now touches an attribute for every match, and the
   50 000-match common term is where that shows up if anywhere.
2. `./gradlew :benchmark:rankAb --configs recency_off,recency_h30_w1,recency_h30_w2`
   over the extended case file, on a staging port-forward.
3. `./gradlew :benchmark:test -Pintegration` green, including the new cases.

If (1) comes back badly — it should not, but `attribute(created_at)` is read
twice per hit on the recency path today and that path is the one that measures
~100 ms per million postings — the fallback is second-phase-only recency with a
raised `rerank-count`, accepting §3.2's blind spot in exchange.

## 7. Rollout

1. **Deploy inert.** §5.1–5.5 with both weights at `0.0`. Every score, every
   position, every match set identical to today, verified by the existing ITs.
   The schema and the Kotlin land together, as they must.
2. **A/B live.** The weights are `query()` inputs — `rankAb` sweeps `w`,
   `recency_halflife` and the kind split against real staging data with no
   redeploy, per the harness's whole reason for existing.
3. **Ship a default** by moving `query(w_recency)` in `event.sd`, with the
   winning numbers and the report they came from written into the comment
   beside it, and the new cases pinned in `RankRegressionIT`.
4. **Kind 0 last, separately.** Only after the note-side default has been live
   long enough to have its own bug reports, and only with the `rank_cases.json`
   ladder re-run — that is the calibrated surface.
5. **Optional, phase 3:** a caller-facing control. The store already owns the
   NIP-50 extension grammar, so `recency:<days>` / `recency:off` mapping to
   `recency_halflife` / `w_recency` is a small `FilterMapping` change. Not in
   v1: an operator-tuned default is what "consider recency in all text matches"
   asked for, and a knob nobody sends is a knob nobody tunes.

## 8. Alternatives considered

- **`sort:recent` as the default.** Exists, and is the thing users are working
  around: it discards relevance entirely (the store strips the text weights for
  that shape). "Tilted", not "replaced", is the request.
- **Vespa's `freshness(created_at)` / `age(created_at)`.** Linear, hard-zeros
  past `maxAge` (no tail for rare queries), and built on `now − t` semantics
  that reward the 2100-dated notes (constraint 4). One divide of our own buys
  the shape we want and the future-timestamp defence.
- **Reading Vespa's `now` directly in the profile.** Cheaper to write, and it
  makes ranking a function of wall-clock: unpinnable tests, per-node
  disagreement, unreplayable reports (constraint 6).
- **An additive recency term in `search`.** The bands there are multiplicative
  and span 680 → 130 100; an additive term that matters at the bio rung is
  invisible at the name rung, and one sized for the name rung obliterates the
  bio rung. This is exactly the mistake the second-phase precision boost made
  before 2026-08-05 (40 raw points = 40 at the trust floor, 0.00017 at trust 100).
- **A decayed score baked in at ingest** (a `recency_boost` field written by
  the writer). Needs a re-feed of 211M documents to retune, ages wrong between
  writes, and adds an attribute to a schema whose budget is documented per
  field. `created_at` is already there.
- **`match-phase` on `created_at` for `search`.** Cuts by time *during*
  matching, so a rare-term query loses its only good hit because it is old.
  Match-phase is for shapes where the cut key **is** the sort key
  (`recency`, `recency_gated`), which is why those two profiles have it and
  none of the text profiles do.
- **A hard `since = now − 90d` window.** Changes recall, not order. NIP-01
  filters already let a client ask for that; a search should not do it behind
  their back.

## 9. Open questions for review

1. **Default strength.** The A/B ladder is w ∈ {0.5, 1, 2}; my prior is
   **w = 1, H = 30d** for kind 1 — a fresh note beats one a year older unless
   that author is ~26 % more trusted. Is "news-shaped" (w = 2) closer to what
   the relay wants?
2. **Kind granularity.** Proposal splits kind 0 from everything else. Long-form
   (30023) plausibly wants a much longer half-life than kind 1, which would mean
   a third class rather than one more weight. Worth it in v1, or after the data?
3. **Should `sort:rank` / `sort:followers` inherit recency as their last
   within-tier tiebreak?** They are explicit sorts, so my answer is no, but they
   are also the tokens people reach for when relevance disappoints them.
4. **`recency:` extension token in v1** (§7.5) — the store owns the grammar and
   `search-staging` is the only consumer that would send it.
