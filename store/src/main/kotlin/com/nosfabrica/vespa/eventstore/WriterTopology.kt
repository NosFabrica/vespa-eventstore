/*
 * Copyright (c) 2026 NosFabrica
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.nosfabrica.vespa.eventstore

/**
 * WHO ELSE WRITES the index this store reads — an assertion about the
 * deployment, not a tuning knob.
 *
 * The store's admission checks are query-then-write, so every guarantee it
 * makes is scoped to what it can see. One thing it caches across the whole
 * process — the set of owners with a stored NIP-09 tombstone / NIP-62 vanish
 * (`GuardOwners`, which lets ~every insert skip both guard probes) — is only
 * self-maintaining while THIS process is the only writer for its owners.
 * A second writer's tombstone never reaches this process's cache, so it would
 * re-admit what the other writer erased, forever. That is what this type makes
 * the caller state.
 *
 * THE CACHE IS PURELY A PERFORMANCE DEVICE. It never makes anything more
 * correct: it only ever decides to SKIP a probe the store would otherwise run,
 * so its every failure is in one direction — an event a tombstone covers gets
 * admitted, stored, and served. And it does not self-correct: re-delivering
 * the tombstone hits the dedup gate before `applyDeletion`, so nothing
 * re-sweeps what was wrongly let in.
 *
 * Which is why the DEFAULT is [SHARED_STRICT], the mode that caches nothing.
 * "A deleted event is never served" is an invariant, not a budget, so a
 * deployment that says nothing about its topology must not be trading it for
 * read capacity. The read savings are opted INTO by asserting a property of
 * the deployment — [SINGLE_WRITER] if it is true (then there is no window at
 * all), [SHARED] if a bounded one is genuinely acceptable.
 *
 * Orthogonal to the per-insert races in docs/multi-node-consistency.md (which
 * no cache setting fixes — those want one write lane per owner).
 */
enum class WriterTopology {
    /**
     * Other writers, or simply no claim about them: NEVER skip a guard probe.
     * Every insert pays the NIP-09/NIP-62 admission reads. **The default.**
     *
     * A covered event is rejected at admission with no staleness window of any
     * kind. Correct under every topology, and the only mode that needs no
     * assertion to be correct — which is exactly why a caller who has not
     * thought about writers lands here. Equivalent to `GUARD_OWNERS_DISABLE=1`
     * (which forces this mode whatever the caller passed).
     *
     * The cost is the measured guard-cache win, given up: reads/event 1.73 →
     * 3.26 on the per-event insert path (docs/server-side-constraints.md). The
     * bulk `batchInsert` path pays too, less obviously — its guard queries are
     * amortized per BATCH rather than per event, but they stop being free and
     * they sit UNDER the writer lock, so the locked share of a commit goes from
     * 2 of 3 round trips to 3 of 4: 8 concurrent disjoint-owner batches
     * serialize 6.6x instead of 5.7x (`BatchIngestConcurrencyTest`).
     */
    SHARED_STRICT,

    /**
     * This process is the ONLY writer for its owners — a single store instance,
     * or one lane of an owner-sharded fleet (docs/multi-node-consistency.md).
     *
     * The guard cache is then exact for the process's lifetime: it is loaded
     * once from the corpus and every guard stored afterwards is this process's
     * own write. No refresh, no background scans. The fast path with NO
     * staleness window — but only while the assertion holds. Assert it falsely
     * and a second writer's tombstones are ignored for as long as this process
     * lives.
     */
    SINGLE_WRITER,

    /**
     * Other processes write the same index, AND a bounded window in which a
     * foreign tombstone is not yet honoured is acceptable for this deployment.
     *
     * The guard cache is kept and rebuilt from the corpus on an interval, so a
     * foreign guard takes effect after at most one refresh instead of never.
     * The hot-path read savings survive, at the cost of one distinct-author
     * scan per guard kind per refresh. `refreshGuardOwners()` forces one.
     *
     * Read the bound honestly before choosing this: it is
     * `max(guardRefreshSeconds, rebuild duration)`, and the rebuild is a
     * document-API visit whose cost scales with the WHOLE corpus, not with the
     * number of guards. On a large corpus that is the binding term, and it is
     * measured in hours. Until the rebuild is index-backed, prefer
     * [SHARED_STRICT] for anything that must not serve a deleted event.
     */
    SHARED,
}

/**
 * How often [WriterTopology.SHARED] rebuilds the guard-owner cache, in millis.
 *
 * A rebuild is one distinct-author scan per guard kind (kind 5, kind 62) — the
 * `GuardOwners` load, which its own doc puts at seconds-to-minutes on a large
 * corpus. So this is a several-minute cadence, not a tight loop: it bounds how
 * long a foreign tombstone can be ignored, and the bound worth paying for is
 * "minutes", not "forever".
 */
const val DEFAULT_GUARD_REFRESH_MILLIS: Long = 300_000L
