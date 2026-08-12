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
 * Admission checks are query-then-write, so every guarantee is scoped to what
 * this process can see. `GuardOwners` — the process-wide cache of owners with a
 * stored NIP-09 tombstone / NIP-62 vanish, which lets ~every insert skip both
 * guard probes — is only self-maintaining while THIS process is the only writer
 * for its owners. A second writer's tombstone never reaches the cache, so it
 * would re-admit what the other writer erased, forever.
 *
 * The cache is purely a performance device: it only ever SKIPS a probe, so its
 * every failure is in one direction (an event a tombstone covers gets served),
 * and it never self-corrects — re-delivering the tombstone hits the dedup gate
 * before `applyDeletion`. Hence the default is [SHARED_STRICT], which caches
 * nothing; the savings are opted INTO by asserting a property of the deployment.
 *
 * Orthogonal to the per-insert races in docs/multi-node-consistency.md, which no
 * cache setting fixes.
 */
enum class WriterTopology {
    /**
     * Other writers, or simply no claim about them: never skip a guard probe.
     * **The default**, and the only mode that needs no assertion to be correct.
     * Equivalent to `GUARD_OWNERS_DISABLE=1` (which forces this mode whatever
     * the caller passed).
     *
     * Measured cost against a live single-node Vespa (benchmark/README §2bb):
     * per-event `insert()` ~143 → ~137 events/sec (−4.5%) with p50 UNCHANGED,
     * nothing measurable on `batchInsert`. Latency is unchanged because
     * `insertLocked` fires the dup probe and both guard probes concurrently.
     * What the cache really bought was engine READ CAPACITY (reads/event
     * 3.26 → 1.73, docs/server-side-constraints.md) — headroom, not speed, and
     * not worth an invariant.
     */
    SHARED_STRICT,

    /**
     * This process is the ONLY writer for its owners — a single store instance,
     * or one lane of an owner-sharded fleet (docs/multi-node-consistency.md).
     *
     * The cache is then exact for the process's lifetime: loaded once from the
     * corpus, and every guard stored afterwards is this process's own write. The
     * fast path with NO staleness window — but assert it falsely and a second
     * writer's tombstones are ignored for as long as this process lives.
     */
    SINGLE_WRITER,

    /**
     * Other processes write the same index, AND a bounded window in which a
     * foreign tombstone is not yet honoured is acceptable. The cache is rebuilt
     * from the corpus on an interval — one distinct-author scan per guard kind
     * per refresh; `refreshGuardOwners()` forces one.
     *
     * Read the bound honestly: it is `max(guardRefreshSeconds, rebuild
     * duration)`, and the rebuild is a document-API visit whose cost scales with
     * the WHOLE corpus, not the number of guards. On a large corpus that is the
     * binding term, measured in hours. Until the rebuild is index-backed, prefer
     * [SHARED_STRICT] for anything that must not serve a deleted event.
     */
    SHARED,
}

/**
 * How often [WriterTopology.SHARED] rebuilds the guard-owner cache, in millis.
 * A rebuild is one distinct-author scan per guard kind, which its own doc puts
 * at seconds-to-minutes on a large corpus — so this is a several-minute cadence,
 * not a tight loop.
 */
const val DEFAULT_GUARD_REFRESH_MILLIS: Long = 300_000L
