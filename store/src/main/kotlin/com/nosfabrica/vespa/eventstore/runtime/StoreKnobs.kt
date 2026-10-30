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
}

/**
 * Ids per dedup probe when nothing sets `VESPA_DEDUP_CHUNK`: the width the
 * store's own guard and version reads are built at (BulkRecordInsert's
 * CHECK_CHUNK), and the one the REQ-latency A/B was measured at.
 */
const val DEFAULT_DEDUP_CHUNK = 500
