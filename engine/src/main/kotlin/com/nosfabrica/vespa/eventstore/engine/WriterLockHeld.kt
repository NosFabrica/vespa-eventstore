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
package com.nosfabrica.vespa.eventstore.engine

import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * A COROUTINE-CONTEXT MARK: the caller holds a store writer lock, and every
 * other writer is queued behind this one's reads.
 *
 * An engine read that would otherwise WAIT OUT a degraded cluster asks once
 * under it and throws: `VespaEventIndex`'s id walk retries a refused page over
 * ~4.2 s (a node redistributing clears on its own), which is the right remedy
 * for a NIP-77 snapshot or the projection drain and the wrong one for a write
 * commit — the supersession version read, the default `putIfNewer` — where it
 * holds the whole ingest still for 4.2 s per page and per tie read, and where
 * the caller (a batch re-offered, an insert retried) already owns the retry.
 * Before the write path read its versions through the walk, those commits
 * failed in one round trip; with this mark they do again.
 *
 * Set by the store's lock helper around every locked body; read by the
 * engine. An implementation with nothing to wait for ignores it.
 */
object WriterLockHeld : AbstractCoroutineContextElement(WriterLockHeld.Key) {
    object Key : CoroutineContext.Key<WriterLockHeld>
}
