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
package com.nosfabrica.vespa.eventstore.runtime

import com.nosfabrica.vespa.eventstore.engine.tuning.ProcessKnob

/**
 * The `:store` side's process-wide settings — the companion constants of the
 * ingest and trust paths, which no store instance threads through (see
 * [ProcessKnob] for why they are pinned once and how a process that pins
 * nothing still reads its environment as before). Each is explained where it is
 * read; `EngineTuning` is the strict way to set them.
 */
internal object StoreKnobs {
    /** `VESPA_DEDUP_CHUNK` — ids per stage-B dedup query; see `BulkRecordInsert.DEDUP_CHUNK`. At least 1. */
    val dedupChunk = ProcessKnob("VESPA_DEDUP_CHUNK", DEFAULT_DEDUP_CHUNK) { it.toIntOrNull()?.coerceAtLeast(1) }

    /** `VESPA_TRUST_WALK_PAGE` — cards per gate hold in a service walk; see `TrustRecompute.PROJECT_PAGE`. At least 1. */
    val trustWalkPage = ProcessKnob("VESPA_TRUST_WALK_PAGE", 250) { it.toIntOrNull()?.coerceAtLeast(1) }

    /** `VESPA_TRUST_GATE_SLICE` — subjects per write-gate hold; see `TrustRecompute.GATE_SLICE`. At least 1. */
    val trustGateSlice = ProcessKnob("VESPA_TRUST_GATE_SLICE", 500) { it.toIntOrNull()?.coerceAtLeast(1) }

    /**
     * `VESPA_EXPIRY_SWEEP_PAGE` — ids the NIP-40 sweep removes per lock hold;
     * see `NostrSemanticsStore.deleteExpiredEvents`. At least 1.
     *
     * 1,000, and not the 10,000 of `sweepPage`, because a hold no longer READS
     * its page: the walk projects ids outside the hold, and the hold is the
     * removes plus the trust kinds' read-back — two `TrustProjection` chunks
     * of 500 (REMOVE_CHUNK) and one feed chunk (FEED_CHUNK, 4,096). A smaller
     * batch no longer multiplies the sweep's total matching, which is what
     * kept `sweepPage` large; it only adds holds. 1,000 is the size the trust
     * walk ran per gate hold before staging measured a 1.1 s max insert wait
     * at it and lowered it to 250 (`VESPA_TRUST_WALK_PAGE`, docs/service-keyed-trust.md);
     * a hold here does no derivation, so it is cheaper per id than that one.
     * NOT measured on a live Vespa — `lock.sweep.hold` in the status line is
     * the number to read before moving it.
     *
     * The trust pass pays one crash-ledger bracket per batch: its removeAll
     * opens a `TrustProjection` commit, which writes the marker ahead and
     * clears it — two reputation writes per batch, against the batch's own
     * cell removals (up to 1,000 at this size: ~0.2% more writes, ten times
     * the brackets of a 10,000 batch). Kept, deliberately. A larger trust
     * batch holds BOTH locks longer, which is what this size bounds; and one
     * bracket for the whole pass would span the releases between batches,
     * where a commit's buffered cell removals could land after a newer card
     * for the same address put its cell, and remove it (as read from
     * `TrustProjection.reacting`, which buffers a commit's removals; not reproduced).
     */
    val expirySweepPage = ProcessKnob("VESPA_EXPIRY_SWEEP_PAGE", 1_000) { it.toIntOrNull()?.coerceAtLeast(1) }
}

/**
 * Ids per dedup probe when nothing sets `VESPA_DEDUP_CHUNK`: the width the
 * store's own guard and version reads are built at (BulkRecordInsert's
 * CHECK_CHUNK), and the one the REQ-latency A/B was measured at.
 */
const val DEFAULT_DEDUP_CHUNK = 500
