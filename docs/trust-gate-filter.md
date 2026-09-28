# The trust gate as a match filter — prototyped, measured, not shipped (2026-09-28)

## The idea

An observer-gated read today runs the `recency_gated` / `recency_gated_exact` profile: every
match is RANKED to read the author's trust cell off the imported reputation tensor, and
`rank-score-drop-limit` drops a below-floor author. That makes a gated COUNT, and a gated read
that cannot be windowed (a deep page, a speculative fallback), cost the whole match set —
~1 µs a match, 5–6 s for a global kind-1 COUNT on a 7.4M-event slice.

The alternative: give the reputation document a `trusted_services` field — the service keys
under which the author clears the store's default floor (`DEFAULT_MIN_RANK`) — import it into
`event`, and write the default gate as `author_trusted_services contains "<lens>"` in the WHERE
clause of an ordinary query. Counts become groupings; gated reads get the plain `recency`
profile and its match-phase cut.

## How it was measured

Two schema lines — not merged; applied to a throwaway engine only:

```
# reputation.sd, in `document reputation`
field trusted_services type array<string> {
    indexing: attribute | summary
    attribute: fast-search      # variant A; variant B omits this line and `dictionary`
    match { cased }
    dictionary { hash }
}

# event.sd, beside the other imports
import field author_ref.trusted_services as author_trusted_services {}
```

A fresh Vespa 8.731 beside the relay's engine, loaded read-only from the 44M-doc local corpus
(`vespa visit --make-feed` piped into `vespa feed`): all 299,414 reputation documents, with
`trusted_services` derived from `influence_scores` at floor 2 (147,948 authors trusted by the
canonical observer), and every kind 1/6/7/9735 event (7,443,283). `benchmark/trust_filter_probe.py`
runs each shape both ways on the same node, asserts the two answers are IDENTICAL (counts; page
ids in NIP-01 order) and times both. Median of 3–5, single match thread.

## Results

Both variants returned the identical answer to the gated profile on every comparison — 7
counts and 42 pages per variant, across global, follow-list, quiet-author, hashtag and
notification shapes, unwindowed, inside a one-hour window, and 180 days deep. The semantics
hold. The cost does not:

| | gated profile (today) | A: filter, parent `fast-search` | B: filter, no `fast-search` |
|---|---:|---:|---:|
| COUNT global kind 1 (5.7M trusted of 6.6M) | 5.4–6.2 s | **0.48 s** (11×) | 4.7 s |
| COUNT follow 300 / 1000 | 1.0 / 1.2 s | **0.34 / 0.38 s** | 0.88 / 0.98 s |
| COUNT `#t:bitcoin` / `#p` | 118–137 / 84 ms | 242 / 239 ms | 93 / 68 ms |
| deep page, global (until −180 d) | 3.9–4.7 s | **0.53 s** (7×) | 3.7–3.9 s |
| unwindowed feed pages | 21–108 ms | 231–322 ms | 14–102 ms |
| pages inside a 1-hour window | 3–12 ms | 3–11 ms | 2–10 ms |

- **A (`fast-search` on the parent field)** answers huge match sets 7–11× faster, but pays a
  FIXED ~230 ms on every query, whatever else narrows it: the imported search appears to
  materialize the child posting list for every document of every trusted parent (5.7M here —
  ~40 ns a document). That fixed cost should grow with the corpus (unverified: the staging
  corpus is ~28× this slice), and it makes selective reads — most traffic — 2–10× slower.
- **B (no `fast-search`)** checks each candidate's parent instead: no fixed cost, but a
  per-candidate price of the same order as the tensor lookup the gate already pays. It lands
  at parity with today (1.0–1.4× on most shapes; unwindowed `#t` pages 3× slower) — no reason to
  carry a schema change.
- **Windows already fixed the common case.** Inside the speculative strategy's one-hour window
  every variant is 3–12 ms. What remains expensive is gated COUNT over huge match sets and the
  deep unwindowed reads speculation falls back to.

## Verdict

Not shipped as a replacement for the gate. What is left worth doing, cheapest first:

1. **NIP-45 `"approximate": true`** for a gated COUNT over a huge match set: the ungated
   grouping count (282 ms here) scaled by a sampled trust ratio. No schema change.
2. **Variant A for COUNT only, behind a size gate**: run the ungated count first; above a
   threshold (hundreds of thousands) count with the fast-search filter, below it keep the
   profile. Measured shape: 282 + 476 ms against 5.4 s for the global count; ~+10% on small
   counts. Needs the schema field, its maintenance in `TrustProjection` (a `weightedset<string>`
   there, so add/remove partial updates are idempotent; one update per card crossing the floor),
   and a re-check of the fixed cost at staging's scale before it is worth it.
3. An explicit `filter:rank:gte:N` floor is not the default floor and would keep the profile
   either way.

Reproduce: deploy `engine/app` with the two lines above to a fresh Vespa, feed it as described,
then `python3 benchmark/trust_filter_probe.py --lens <key> --vespa <its query URL>`. Switching A ↔ B
is a redeploy plus a content-node restart (Vespa asks for one), no refeed.
