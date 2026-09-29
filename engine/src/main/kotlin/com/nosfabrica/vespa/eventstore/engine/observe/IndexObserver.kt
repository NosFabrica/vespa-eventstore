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
package com.nosfabrica.vespa.eventstore.engine.observe

import com.vitorpamplona.quartz.nip01Core.core.Event

/**
 * Told about every ACKED physical write to the event index — the hook a
 * mirror of this store (a graph projection, a second index) hangs on to stay
 * exact. Installed by `VespaEventStore.open(observers = …)`, which wraps the
 * engine in an [ObservedEventIndex].
 *
 * WHY THE PORT, AND NOT THE STORE'S INSERT/DELETE: every mutation this store
 * makes — an insert, and every removal style (replaceable supersession, NIP-09
 * targets, NIP-62 vanish, NIP-40 expiry sweeps, `delete(filter)` / NIP-86
 * purges, the orphan-score sweep) — funnels through `EventIndex`, which is the
 * same fact `TrustProjection` rests on. Observing the seam sees a removal style
 * added next year the day it is written; observing the store's surface would
 * see only the ones someone remembered to wire.
 *
 * WHY IT LIVES IN `:engine` although a consumer names it: the decorator that
 * calls it wraps the engine port, and `:engine` may not import `:store`
 * (`ModuleBoundariesTest`). `:store` exposes `:engine` as `api`, so a consumer
 * holding `VespaEventStore` names this type with no extra dependency.
 *
 * THE CONTRACT, from the write path's side:
 *  - Called on the WRITER'S coroutine, after the engine acked, often while the
 *    store's writer lock is held. Implementations MUST NOT BLOCK: hand the
 *    batch to a queue and return. A slow observer is a slow ingest.
 *  - Possibly CONCURRENTLY: the store's writes are not all serialized behind
 *    one lock (docs/locking.md — the trust gate and the event writer are
 *    separate), and a consumer may assemble its own stack with no lock at all.
 *    Implementations must be thread-safe, and calls from different writers
 *    arrive in no promised order; within one call the order is the order the
 *    write was issued in.
 *  - Implementations MUST NOT THROW. A throw is caught, counted
 *    ([ObservedEventIndex.observerFailures]) and dropped — the write it
 *    describes has already landed, so failing it would report a stored event as
 *    rejected. The mirror then misses that batch; the read-only walks on
 *    `EngineReads` (`visitIds`, `visitDocsPage`) are how it reconciles.
 *  - AT-LEAST-AS-ASKED, not exactly-what-changed: [onRemove] carries every id a
 *    removal was issued for, including ids the index did not hold, and
 *    [onPut] may repeat an event already stored (a full-text reindex re-puts
 *    documents unchanged). A mirror must apply both idempotently.
 *  - A call that FAILED reports nothing, though a bulk one may have partially
 *    landed before it threw. Reconcile for that too.
 */
interface IndexObserver {
    /**
     * Events now stored. Plain Quartz [Event]s built from the stored fields
     * (not kind-typed subclasses), signature included; a mirror that needs the
     * typed view can run them through Quartz's `EventFactory` itself.
     */
    fun onPut(events: List<Event>)

    /** Event ids removed from the index — by any route (see the class KDoc). */
    fun onRemove(ids: List<String>)
}
