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
 * It is deliberately NOT inferable: no store can see another process feeding
 * the same cluster. Defaulting to [SHARED] therefore means the deployment that
 * says nothing gets bounded staleness rather than an unbounded one, and the
 * process-lifetime cache is something a deployment opts into by asserting a
 * property of itself.
 *
 * Orthogonal to the per-insert races in docs/multi-node-consistency.md (which
 * no cache setting fixes — those want one write lane per owner).
 */
enum class WriterTopology {
    /**
     * This process is the ONLY writer for its owners — a single store instance,
     * or one lane of an owner-sharded fleet (docs/multi-node-consistency.md).
     *
     * The guard cache is then exact for the process's lifetime: it is loaded
     * once from the corpus and every guard stored afterwards is this process's
     * own write. No refresh, no background grouping queries. The fastest and,
     * under the asserted topology, fully sound.
     */
    SINGLE_WRITER,

    /**
     * OTHER processes write the same index (a sync router beside a relay, a
     * second store instance, any foreign feeder) — the default, because a
     * library cannot detect them.
     *
     * The guard cache is kept, but rebuilt from the corpus on an interval, so a
     * foreign tombstone is honoured after at most one refresh instead of never.
     * Staleness becomes the interval, not the process lifetime; the hot-path
     * read savings survive, at the cost of one distinct-author scan per guard
     * kind per refresh, off the insert path. `refreshGuardOwners()` forces one.
     */
    SHARED,

    /**
     * Other writers, and NO staleness window is acceptable: never skip a guard
     * probe. Every insert pays the NIP-09/NIP-62 admission reads.
     *
     * The strict floor, equivalent to `GUARD_OWNERS_DISABLE=1` (which forces
     * this mode whatever the caller passed). Correct under any topology; it
     * gives up the measured reads/event win entirely (3.26 → 1.73, see
     * docs/server-side-constraints.md), which is why it is not the default.
     */
    SHARED_STRICT,
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
