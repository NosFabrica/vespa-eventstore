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

import com.nosfabrica.vespa.eventstore.engine.client.EngineResources
import com.nosfabrica.vespa.eventstore.engine.metrics.CostLedger
import com.nosfabrica.vespa.eventstore.engine.metrics.DegradedReads
import com.nosfabrica.vespa.eventstore.engine.metrics.IngestStats

/**
 * EVERYTHING A STATUS PAGE READS OFF THIS STORE, in one value taken at one
 * instant — [VespaEventStore.telemetry].
 *
 * A pulse page used to assemble it from five places: the store's [metrics]
 * plus four process-wide registries in the engine ([IngestStats]' stages,
 * holds and blocked split, [DegradedReads]), each read when the page's code
 * got round to it, so the lock table and the stage table on one page described
 * different moments, and the page knew the engine's internals by name. The
 * registries stay where they are; the snapshot is here.
 *
 * ONE INSTANT means back to back in one call, after the one read that does
 * I/O ([headroom], which is cached and may take a round trip): the registries
 * are lock-free, so this is not a transaction across them, but every member
 * describes the same microseconds rather than whatever a page's render order
 * made them. Every field is cumulative or instantaneous exactly as its source
 * documents — a window is still the difference of two snapshots.
 */
class StoreTelemetry internal constructor(
    /** When the counters below were read, epoch millis. */
    val atMillis: Long,
    /** This store's cost ledger: activities, port calls, engine altitude, gauges, outcomes — `VespaEventStore.metrics()`. */
    val metrics: CostLedger.Snapshot,
    /** The write path's stage split, process-wide — `IngestStats.snapshot()`. */
    val stages: Map<String, IngestStats.Stage>,
    /** Every writer-lock hold in flight, oldest first, process-wide — `IngestStats.heldAll()`. */
    val held: List<IngestStats.Held>,
    /** What ingest was waiting behind: lock -> holder stage -> waits, process-wide — `IngestStats.blockedSplit()`. */
    val blocked: Map<String, Map<String, Long>>,
    /** Engine answers that came back degraded, by profile and shape, process-wide — `DegradedReads.snapshot()`. */
    val degraded: List<DegradedReads.Reading>,
    /** The feed client's health line — `VespaEventStore.feedStatus()`. */
    val feed: String,
    /** The background workers' progress and failures — `VespaEventStore.backgroundStatus()`; empty while healthy and idle. */
    val background: String,
    /** The content nodes' memory and disk against the feed-block limits; null when the probe has never answered. */
    val headroom: EngineResources.Usage?,
)
