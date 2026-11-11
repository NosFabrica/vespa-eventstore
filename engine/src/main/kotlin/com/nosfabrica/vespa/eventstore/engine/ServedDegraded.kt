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

import kotlinx.coroutines.currentCoroutineContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * WHETHER A READ WAS SERVED PART OF THE CORPUS: installed by a caller that needs to know, marked
 * by the index when it serves an answer missing documents it did not search (a node down, let
 * through by `EventQuery.acceptsPartial`). A recency profile's own match-phase cut does not mark:
 * the recall proves or reruns that page. A refused answer throws instead and marks nothing.
 *
 * Part of the port: an [EventIndex] that can serve a degraded answer calls [markCurrent] when it
 * does. One that cannot (the in-memory reference) never marks, which is the truth for it.
 */
class ServedDegraded : AbstractCoroutineContextElement(ServedDegraded) {
    @Volatile var seen: Boolean = false
        private set

    fun mark() {
        seen = true
    }

    companion object Key : CoroutineContext.Key<ServedDegraded> {
        /** Marks the tracker installed around the calling coroutine, if there is one. */
        suspend fun markCurrent() {
            currentCoroutineContext()[ServedDegraded]?.mark()
        }
    }
}
