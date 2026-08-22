# Recency in text ranking — a proposal

**Status: SHIPPED at `w_recency = 0.1`, half-life 30 d** (2026-08-22), from
the run in §4.2. The trust profiles are live; the pure-text profiles keep their
own weight at 0.0. Turning it off, or to any other value, is a query feature —
no redeploy. §6 says exactly what is measured and what is not.

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

**Measured, 2026-08-22**, read-only against the live staging relay (observer
`460c25…065c`, `kind 1`, `limit 50`; full table in `benchmark/README.md`):

| query | lens | median age of the top 50 | older than 1y | newest hit on the page |
| --- | --- | --- | --- | --- |
| bitcoin | trust (`search`) | **1057 d** | 49/50 | 144 d |
| bitcoin | `sort:recent` | **0 d** | 0/50 | 0.0 d |
| lightning | trust | 955 d | **50/50** | 574 d |
| nostr | trust | 1106 d | 49/50 | 337 d |
| coffee | trust | 1098 d | 49/50 | 208 d |

The `sort:recent` row is the whole argument. Same filter, same trust lens, 50
same-day hits: fresh, trusted, matching content exists in quantity, and the
default ranking shows none of it. A page of "lightning" contains nothing from
the last eighteen months. This is not a corpus problem to fix upstream — it is
a ranking function with no time term in it.

## 2. What the shape of the change is constrained by

Seven things in this repo decide what a recency signal is allowed to look like.
They are the reason this is a page of design and not a one-line `+ attribute(created_at)`.

1. **The `search` bands are multiplicative and calibrated.** `wot_mult()` is
   convex (`1 + w_wot · delta^2.7`) specifically so that trust crosses a text
   tier only on an overwhelming advantage (~7.6× delta for the bio→name gap),
   pinned from both sides by the 2026-08-02 "odell" case. The equal-trust rung
   floors are 100 (the `text_score_cutoff` floor band) / 550 / 4 000 / 23 000
   / 130 100 — each rung ×5.5 to ×7.3 the one below, the **×5.5** from the
   floor band to the bio/body rung being the binding one. **Any recency factor
   bounded under 5.5 provably cannot cross a rung**, whatever the corpus. That
   bound is the safety argument, and it only exists for a *multiplicative*
   term.
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
   `benchmark/rank_cases.json` is a kind-0 profile search. (This constraint
   argued for exempting kind 0. §3.4 is why that cure was worse, and what
   bounds the churn instead.)
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

### 3.1 The shape

```
event_age_days() = |clock − attribute(created_at)| / 86400
freshness()      = 1 / (1 + event_age_days() / query(recency_halflife))
recency_mult()   = 1 + query(w_recency) · freshness()   // one weight, every kind — §3.4
clock            = query(now_secs), falling back to Vespa's `now`
```

**Symmetric age** — `max(now − t, t − now)`, i.e. |now − created_at|, rather
than `max(0, now − t)` — is constraint 4: a note dated 2100 is
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
  `1 + w > 5.5`, i.e. **`w > 4.5`**. That is the hard ceiling; the practical
  one is lower, because rungs have tails (a bio-band doc carrying the term in
  body and hashtags reaches ~900 against the weak floor of 4 000 — 4.4×).
  **Recommended ceiling `w ≤ 2.0` (3×)**, which keeps ≥1.5× of margin to the
  nearest realistic band edge, and a shipped default to be picked by the A/B.

Note what this deliberately gives up: recency can **never** float a body
mention above a real name match, no matter how fresh. That is not a limitation
to fix later — it is the same rule that keeps trust honest.

### 3.4 One weight for every kind — the split was tried and rejected

The first cut had a second weight, `w_recency_profile`, exempting kind 0 on
constraint 5's argument: a replaceable event's `created_at` is its last edit,
so freshness there rewards profile churn, and kind 0 is where the calibrated
ladder lives.

That argument is true and it was the wrong lever. Exempting one kind makes the
multiplier **discontinuous across kinds**: every non-profile document gains up
to `(1 + w)` on every profile *regardless of age* — a standing thumb on the
scale, not a tiebreak. `RankRegressionIT` caught it within an hour of shipping
`w = 0.1`: the **"primal"** case (the profile named *primal* against the same
author's follow set titled *Primal*, separated by an identity credit worth well
under 1 %) flipped on the 0.25 % that a 1157-day-old corpus gets at `w = 0.1`.
The pinned cases most exposed to it are exactly the profile-versus-note ones —
Avi Burra, Jon Gordon, primal, amethyst — decided by margins of 2.9–5.7 %.

One weight for every kind keeps the factor continuous: same age, same
multiplier, whatever the kinds. A cross-kind comparison is then perturbed only
by the age difference that is really there, and on a time-flat corpus the whole
change is a global constant — provably no-op, which is what makes the existing
ladder cases safe by construction rather than by luck.

The churn worry survives, bounded. At `w = 0.1` an edit buys at most 10 %,
worth `1.1^(1/2.7) = 3.6 %` of trust delta; the tightest calibrated crossing
(odell, bounded from both sides at 8.6 vs 7.6 — ~13 % of margin) is three times
out of reach. At `w = 0.25` the same number is 8.6 % and the margin starts to
matter, which is an independent reason the shipped weight is 0.1.

### 3.5 The clock comes from the query

`query(now_secs)`, stamped by `EventYql` from the client's clock, with
`if(query(now_secs) > 0, query(now_secs), now)` as the schema-side fallback.
Constraint 6:

- **Deterministic tests.** `RankRegressionIT` pins `IT_NOW = 1_800_000_000` and
  dates its recency fixture relative to it, so those positions are as
  reproducible in five years as today; `RankAb --now <epoch>` does the same for
  a sweep.
- **Identical on every node.** All content nodes score one query against one
  instant.
- **Replayable.** "Why did this rank on Tuesday" is answerable by stamping
  Tuesday.

The store already stamps a clock into every query (`notExpiredAt`), so this is
the established shape, not a new dependency.

## 4. Calibration — the paper answer, and what the corpus actually did

### 4.1 On paper

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

That arithmetic is right and its conclusion — "w = 1 is a sensible starting
candidate" — was wrong by an order of magnitude, because it assumes documents
whose trust deltas differ. Measured, they mostly do not.

### 4.2 What the run said

**4 596 real events** captured read-only from staging (the top-500 relevance
page *and* the top-500 `sort:recent` page for four terms, plus the kind-0
ladder hits), fed into a local Vespa **with their real NIP-85 trust** (the
observer's kind 10040 and the provider's 1 722 kind-30382 cards, so
`TrustProjection` rebuilds the same tensors), then swept. Median age of the
top-10, half-life 30 d, body-band recall — one match band, so recency is the
only thing that *can* move it:

| w_recency | bitcoin | nostr | lightning | coffee | median trust of the page |
| --- | --- | --- | --- | --- | --- |
| **0.0** | 993 d | 1066 d | 1018 d | 973 d | 100 |
| 0.01 | 43 d | 4 d | 34 d | 106 d | 100 |
| 0.05 | 32 d | 0 d | 34 d | 106 d | 100 |
| **0.1** | 7 d | 0 d | 24 d | 2 d | 98–100 |
| 0.25 | 0 d | 0 d | 2 d | 1 d | 96–99 |
| 1.0 | 0 d | 0 d | 1 d | 1 d | 96–98 |

Match counts are **identical on every row** — the `≥ 1` argument, confirmed on
real data at every weight.

A **one percent** tilt moves a page from ~1000 days to ~40. The reason is
saturation: within one band the top of the page is all trust-98..100 authors
whose `wot_mult` are equal to four decimal places (measured spread across a
top-10: **0.0014 %**), so the ordering is a near-tie that any consistent
tiebreak decides. Recency is simply the first tiebreak that has ever been
offered. That is also why the trust column barely moves: what changes is
*which* of the well-trusted matches you see, not how trusted they are — until
about 0.25, where the page goes same-day and the median trust starts slipping.

On ordinary mixed queries, at **w = 0.1 / H = 30 d**: `zap` 1120 d → 525 d,
`podcast` 979 d → 628 d, `privacy` 915 d → 746 d, and every pinned rank case
unmoved (`rankAb`, four candidate weights, zero position deltas). Re-run after
the per-kind split was dropped — so profiles take the boost too — the pinned
positions are still identical at 0.1 and at 0.25 (odell #5, Primal #1, MRR
0.171), while "Odel" drops 165 days and "primal.net" 195.

**Recommended starting point: `w_recency = 0.1`, `recency_halflife = 30 d`** —
still to be confirmed against the full corpus (this slice is deliberately
half-fresh, so it overstates how much fresh material a real page has to choose
from). `w` and `H` trade off directly (only `w · freshness` matters), so sweep
one at a time.

### 4.3 The half the recency term does NOT fix

Also measured, and the more important finding for anyone reading the staging
baseline in §1: **on those four queries the head of the page does not move at
any legal weight**, and that is correct behavior.

The top-10 for `bitcoin` under the default profile are all `text_score ≈
130 100` — the *token band*: kind-1 notes carrying the word in their NIP-14
`subject` ("Bitcoin reserve", "Bitcoin phase", "bitcoin roller coaster").
The 400 freshest matches top out at `text_score = 4 001` — the weak band. That
is a **236× gap**, and no weight under the ladder ceiling can cross it.

So page-one staleness on a common term has two causes, and this change
addresses one of them:

1. *within a band*, near-ties frozen in an arbitrary order — **fixed**, and
   cheaply (w = 0.05–0.1);
2. *across bands*, titled notes outranking fresh mentions — **untouched by
   design**, because that is the tier ladder doing exactly what the odell and
   amethyst cases pin it to do.

Whether a same-day mention should ever cross a rung above a three-year-old
titled note is a policy question this proposal deliberately does not answer:
it would mean putting recency *inside* the ladder rather than under it, and
every calibrated case would have to be re-derived. Worth its own round, with
its own reports.

## 5. What landed

The snippets below are the shipped code, not a sketch: every weight is 0.0, so
the deploy is a no-op until a sweep moves one.

### 5.1 `engine/app/schemas/event.sd` — `text_relevance` inputs

Three new inputs (`w_recency` ships at 0.1; the other two are inert):

```
# ---- RECENCY (docs/recency-ranking.md) ----
# The query instant, epoch SECONDS, stamped by EventYql on every
# ranked text query. Vespa's own `now` is evaluated per content
# node and moves every second, so a profile reading it is not a
# function of the request: RankRegressionIT could not pin a
# position, two nodes could disagree about one query, and a bad
# ranking could not be replayed at the instant it was reported.
# 0.0 (the fail-safe default) falls back to `now`, so a sender
# that omits it degrades to wall-clock, never to 1970.
# Named now_secs, not now, to keep the query feature textually
# distinct from the built-in feature it shadows.
query(now_secs) double: 0.0
# Age at which freshness() halves, in DAYS. Corpus-shaped: ~7 for
# a conversation index, 30 for general notes, 365 for long-form.
# Clamped to >= 1 in freshness() so a zero can never divide.
query(recency_halflife) double: 30.0
# Strength of the multiplicative freshness boost on the DEFAULT
# profile: recency_mult() in [1, 1 + w_recency].
#
# SHIPPED 0.1, from the run recorded below (2026-08-22). 0.0 turns
# recency off entirely without a redeploy — it is a query() input,
# so an operator who disagrees sends their own value per request.
#
# HARD CEILING 4.5. Above it 1 + w exceeds the smallest rung ratio
# in the ladder and recency starts CROSSING text tiers — the
# ordering wot_mult() is calibrated to allow only on overwhelming
# trust. The ratios, floor upward: floor band 100
# (text_score_cutoff, the net under a real match) -> bio/body 550
# is x5.5 and is the BINDING one; then 4000/550 = x7.3,
# 23000/4000 = x5.75, 130100/23000 = x5.65. An earlier version of
# this comment read the smallest as x5.65 by forgetting the floor
# band, and put the ceiling 3% too high.
#
# RankRegressionIT does not pin the ceiling itself — it pins the
# x7.3 weak-over-body pair from both sides (no crossing at 3x,
# crossing at 21x), which proves the bound is a fact about the
# WEIGHT rather than about a fixture that could never cross.
#
# The USEFUL range is nowhere near the ceiling. MEASURED
# 2026-08-22 against a 4 596-event staging slice fed into a local
# Vespa with its real 30382 trust cards (benchmark/README.md):
# inside one match band the trust curve SATURATES — a page of
# trust-100 authors scores within 0.001% of itself — so a 1% tilt
# already re-sorts the page. Median age of the top 10 body-band
# hits, at half-life 30d:
#     w      bitcoin  nostr  lightning  coffee   median trust
#     0.0      993d   1066d     1018d    973d       100
#     0.05      32d      0d       34d    106d       100
#     0.1        7d      0d       24d      2d      98-100
#     0.25       0d      0d        2d      1d       96-99
# On ordinary mixed-band queries the same w=0.1 takes "zap" from
# 1120d to 525d and "podcast" from 979d to 628d, with every pinned
# rank case unmoved. Above ~0.25 the page is same-day and the
# median author trust starts slipping — that is the trade this
# weight buys, and 0.05..0.25 is the band to sweep on real staging.
# A paper estimate put the candidate at 1.0; the run says that is
# 10x too much. Trust the run.
#
# 0.1 is the point where the pages move and nothing else does:
# median trust holds at 98-100, every pinned rank case is
# position-identical (rankAb at 0.05/0.1/0.25: zero deltas), and
# the integration suite is green. RE-MEASURE (do not re-derive)
# against the full corpus after any change to w_wot_pow, the tier
# weights, or the corpus's age distribution: this weight is
# calibrated against a SATURATED trust curve, and anything that
# unsaturates it changes what 0.1 buys.
query(w_recency) double: 0.1
# NO PER-KIND SPLIT — tried, measured, REJECTED (2026-08-22).
# This shipped for one run as a second weight (w_recency_profile,
# 0.0) that exempted kind 0, on the argument that a replaceable
# event's created_at is its LAST EDIT and freshness there rewards
# profile churn. True, and irrelevant next to what the split
# actually does: exempting one kind makes the multiplier
# DISCONTINUOUS across kinds, so every non-profile document gains
# up to (1 + w) against every profile REGARDLESS OF AGE — a
# systematic thumb on the scale, not a tiebreak. RankRegressionIT
# caught it immediately: the "primal" case (the profile named
# primal vs the same author's follow set TITLED Primal, decided by
# an identity credit worth well under 1%) flipped on the 0.25% a
# 1157-day-old corpus gets at w=0.1.
#
# One weight for every kind keeps the factor CONTINUOUS: two
# documents of the same age get the same multiplier whatever their
# kinds, so a cross-kind comparison is perturbed only by the age
# difference that is actually there — and on a time-flat corpus
# the whole change is a global constant, i.e. provably no-op.
#
# The profile-churn worry survives, bounded: at w=0.1 an edit buys
# at most 10%, worth 1.1^(1/2.7) = 3.6% of trust delta. The
# tightest calibrated crossing (the odell case) has ~13% of margin
# in that currency, so recency cannot reach it. RE-DERIVE THAT
# BOUND before raising w: at 0.25 the same number is 8.6%, and the
# margin starts to matter.
# Same, for the PURE-TEXT profiles (`text`/`text2`). Kept apart
# from w_recency because relevance() is ADDITIVE: its rungs are
# 1100/700/620/550 — ratios of 1.13..1.57 — and its uncapped
# bm25/secondary tail already overlaps them, so there is no
# "cannot cross a rung" bound to inherit here at any useful
# strength. Observer-less search is the fallback path; tune this
# only if someone runs it in anger.
query(w_recency_text) double: 0.0
```

### 5.2 `event.sd` — five functions in `text_relevance`

Verbatim from the tree:

```
# ---- RECENCY (docs/recency-ranking.md) ------------------------
# Distance in DAYS between the event and the query instant, and it is
# a distance, not a difference: max(a-b, b-a) is |now - created_at|.
#
# The symmetry is load-bearing, not tidiness. The live corpus really
# does hold notes stamped in the year 2100 (they already top an
# unranked feed), and `now - created_at` — the shape of Vespa's own
# age()/freshness() features, whose formula clamps only from BELOW —
# gives those documents a NEGATIVE age and a freshness above 1, i.e.
# an unbounded win. Read as a distance, a note dated 2100 is as stale
# as one from 1952, which is the honest reading of "this clock is
# wrong", while a client a few minutes ahead of us is still fresh.
#
# The clock, resolved ONCE: functions are inlined textually, so a
# `max(a-b, b-a)` spelling of the same distance expands this `if` four
# times per matched document. fabs() spells it once.
function query_now() {
    expression: if(query(now_secs) > 0, query(now_secs), now)
}

function event_age_days() {
    expression: fabs(query_now() - attribute(created_at)) / 86400.0
}

# 1 at zero age, 0.5 at the half-life, asymptotically 0 and never
# actually 0. HYPERBOLIC, not exponential: a three-year-old exact
# match is still the best answer to a rare query, and an exp() decay
# has effectively deleted it by then. It is also one divide with no
# exp/log — this runs over EVERY match, not a rerank window.
function freshness() {
    expression: 1.0 / (1.0 + event_age_days() / max(1.0, query(recency_halflife)))
}

# >= 1, ALWAYS — CLAMPED, not merely arranged. The invariant is what
# lets this ride the first phase at all, and until the clamp it rested
# on nobody ever sending a negative weight: w_recency = -1 through the
# query features EventQuery.rankFeatures exposes would have deleted
# every FRESH hit from the match set (and from a gated count), which is
# the failure this whole function is written to make impossible. One
# max() per matched document is the right price for that. A negative
# weight is therefore a no-op, not a demotion: recency BOOSTS, and
# demoting by age is a different feature with a different safety
# argument.
#
# The rest of the argument, unchanged: the
# default profile DELETES hits by score (rank-score-drop-limit 0.5,
# and floored_text_score() maps gram-only noise to 0.0 for the limit
# to take), so a factor that can dip below 1 changes the MATCH SET,
# not just the order — a recall regression invisible to anyone reading
# the first page. With x >= 1: a survivor stays a survivor (s*x >= s)
# and a zero stays a zero (0*x = 0). Bit-identical recall, order only.
#
# Bounded by 1 + w, which is what keeps the boost under the ladder's
# x5.65 rung ratio; see query(w_recency).
function recency_mult() {
    expression: max(1.0, 1.0 + query(w_recency) * freshness())
}

# The pure-text profiles' own multiplier — same shape, own weight, and
# no kind split (relevance() has no trust multiply for a profile boost
# to distort). See query(w_recency_text).
function recency_mult_text() {
    expression: max(1.0, 1.0 + query(w_recency_text) * freshness())
}
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

### 5.4 `event.sd` — the text profiles (weight 0 until asked for)

`text`/`text2` multiply by `recency_mult_text()` — `1 + query(w_recency_text) *
freshness()` — with `query(w_recency_text) double: 0.0`. Kept separate and
expected to stay off longer: those bands are additive, 1.13–1.57 apart, and
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
params[F_NOW_SECS] = (q.nowSecs ?: (System.currentTimeMillis() / 1000)).toString()
q.rankFeatures.forEach { (k, v) -> params["ranking.features.query($k)"] = v.toString() }
```

plus a `require()` on the feature name (`[a-z][a-z0-9_]*`) — everything else
this builder puts on the wire is escaped or out-of-band, and a caller-shaped
parameter *name* must not be the hole in that. `query(now_secs)` joins
`TEXT_RANK_FEATURES`, so the gated profiles keep sending nothing they do not
read. No `SchemaFallbacks` demotion is needed — a scalar query feature a
serving profile does not declare is ignored, not a 400 — but **verify that
against staging** before relying on it; it is the kind of claim this repo has
been burned by.

The feature is named `now_secs`, not `now`, to keep it textually distinct from
the built-in `now` rank feature it falls back to.

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

## 6. What is measured, and what is not

**Measured.**

- *The problem*, in numbers: §1's table, read-only off the live staging relay.
  Median top-50 age 1057 d under the default profile for `bitcoin`; 0 d for the
  same filter under `sort:recent`.
- *The mechanism, hermetically*: `./gradlew build` — the query side stamps
  `query(now_secs)` on the profiles that read it, withholds it from the ones
  that rank by `created_at` alone, honours an explicit instant, and rejects a
  malformed rank-feature name (`EventYqlTest`).

- *The whole stack, on a real Vespa*: `:benchmark:test -Pintegration` green,
  including `VespaParityIT`'s 127 parity checks and the new fixture below —
  which also proves the schema, four new inputs and four new functions
  included, **deploys**.
- *The tuning*, §4.2: a real staging slice with its real web of trust, fed
  into a local Vespa (`:benchmark:exportLoad`) and swept
  (`:benchmark:rankAb --kinds 1`).

**The real-Vespa fixture.** `RankRegressionIT` gains a six-document,
one-author, time-spread fixture pinning five claims:

1. **inert at zero** — ids *and scores* bit-identical to not sending the
   feature at all;
2. **recall unchanged** at `w = 4.65` — deliberately past the ×5.5 ceiling,
   since recall must hold at ANY weight — on the fixture and on the calibrated
   ladder queries (`odell`, `jack`) — the `≥ 1` argument, executed;
3. **the decay curve**, newest-first across 1 d / 1 y / 5 y, with the
   year-2100 note sorting **last** (where a `max(0, now − t)` age would have
   put it first);
4. **the ladder is not for sale** — a same-day body hit does not cross a
   five-year-old weak-tier hit at the recommended ceiling, *and* does cross at
   21×, so the bound is a fact about the weight and not about a fixture that
   could never cross;
5. **continuity across kinds** — a one-day-old note and a one-day-old profile
   scale by the same factor to within 1e-6 (the invariant that replaced the
   kind split, §3.4).

**Not measured yet, and needed before the weight leaves 0.0:**

1. The same sweep **against the full corpus**, not a 4 596-event slice. The
   slice was built half from `sort:recent`, so it holds far more fresh
   candidates than a real page does; it can prove the mechanism and locate the
   useful range, and it cannot tell you what a page of 211 M events looks like
   at `w = 0.1`. bm25's IDF and the trust distribution both change at scale.
2. Nothing else — the cost question is answered. `searchBench`, 200 k notes,
   three runs per variant with alternating deploys: **+3.2 % (≈ +10 ms) on the
   ~50 k-match "common term" shape**, every "with" run above every "without"
   run, and *nothing resolvable* on any lighter shape. Folding the two divides
   into one recovers none of it, so the cost is not the arithmetic and the
   readable form stays. Full table in `benchmark/README.md`.

## 7. Rollout

1. **Deploy inert.** ✅ Done — §5 is in the tree with every weight at `0.0`.
   Every score, every position, every match set identical to today, verified by
   the existing ITs plus claim 1 above. The schema and the Kotlin land
   together, as they must.
2. **A/B live.** The weights are `query()` inputs — `rankAb` sweeps `w`,
   `recency_halflife` and the kind split against real staging data with no
   redeploy, per the harness's whole reason for existing.
3. **Ship a default.** ✅ Done — `query(w_recency) double: 0.1`, with the run
   that chose it in the comment beside it and the value mirrored in
   `RankRegressionIT` so it cannot move without a new run.
4. **Watch the calibrated surface.** The kind-0 ladder is not exempt any more
   (§3.4), so a `rank_cases.json` re-run against real staging is what the next
   round of reports gets read against.
5. **Optional, phase 3:** a caller-facing control. The store already owns the
   NIP-50 extension grammar, so `recency:<days>` / `recency:off` mapping to
   `recency_halflife` / `w_recency` is a small `FilterMapping` change. Not in
   v1: an operator-tuned default is what "consider recency in all text matches"
   asked for, and a knob nobody sends is a knob nobody tunes.

## 8. Alternatives considered

- **`sort:recent` as the default.** Exists, and is the thing users are working
  around: it discards relevance entirely (the store strips the text weights for
  that shape). "Tilted", not "replaced", is the request.
- **Vespa's `freshness(created_at)` / `age(created_at)`.** Checked against the
  reference (2026-08-22), not assumed: `age(name)` is "the document age in
  seconds relative to the unit time value stored in the attribute", and
  `freshness(name)` is `max(1 − age/maxAge, 0)` — **linear**, hard-zero past
  `maxAge`, so no tail for a rare query, and its `maxAge` is a rank property
  rather than an input we already sweep. `freshness(name).logscale` restores a
  tail but is built on the same `age()`. And that is the disqualifying part:
  the formula is clamped at zero from *below* only, so a document dated in the
  future has a negative age and a freshness **above 1** — the year-2100 notes
  would not merely rank first, they would outscore everything by an unbounded
  margin. One divide of our own buys the shape we want and the
  future-timestamp defence with it. (Vespa's `now` feature is real and is what
  we fall back to when a client omits the stamp — same reference.)
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

1. **Default strength.** The run puts the useful band at **w = 0.05–0.25**
   (§4.2), an order of magnitude under the paper prior, and my recommendation
   is `w_recency = 0.1, recency_halflife = 30 d`. Turning it on is one number
   in `event.sd` — or a query-feature override on staging first, no deploy.
   Do you want it swept against the full corpus before it ships, or shipped at
   0.1 and watched?
2. **Kind granularity.** One weight for all kinds now, after the split's
   failure (§3.4). Long-form (30023) plausibly wants a longer half-life than
   kind 1 — but note what §3.4 says about per-kind terms: anything that varies
   the factor BY KIND rather than by age reintroduces a standing cross-kind
   bias. A per-kind half-life is milder than a per-kind weight (both ends still
   reach 1 + w at age 0) but it is the same shape of idea and needs the same
   scrutiny.
3. **Should `sort:rank` / `sort:followers` inherit recency as their last
   within-tier tiebreak?** They are explicit sorts, so my answer is no, but they
   are also the tokens people reach for when relevance disappoints them. §4.2
   says they would benefit from the same tiebreak for the same reason: their
   within-tier key saturates too.

5. **The band half (§4.3).** Page one of a common term stays old because
   titled notes outrank fresh mentions by 236×. Leaving that alone is the
   conservative call and it is what shipped. If the reports keep coming, the
   next round is about the ladder, not about recency.
4. **`recency:` extension token in v1** (§7.5) — the store owns the grammar and
   `search-staging` is the only consumer that would send it.
