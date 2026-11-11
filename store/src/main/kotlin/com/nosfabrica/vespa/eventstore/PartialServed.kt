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

import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * WHETHER A READ UNDER [AcceptPartial] WAS SERVED PART OF THE CORPUS: install one beside it, and
 * read [served] once the read has answered.
 *
 * ```kotlin
 * val report = PartialServed()
 * withContext(AcceptPartial + report) { store.rawQuery(filters, onEach) }
 * if (report.served) tellTheClient()
 * ```
 *
 * True when any engine read behind the answer (the recall, the search expansion, the rerank, the
 * lens) came back missing documents the cluster could not search, and was served under
 * [AcceptPartial] rather than refused. A recency profile's own match-phase cut is not partial: the
 * store proves or reruns that page. Never reset by the store, so one instance can span several
 * reads and say whether any of them was partial.
 */
class PartialServed : AbstractCoroutineContextElement(PartialServed) {
    @Volatile var served: Boolean = false
        internal set

    companion object Key : CoroutineContext.Key<PartialServed>
}
