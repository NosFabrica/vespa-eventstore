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

import kotlin.coroutines.CoroutineContext

/**
 * SERVE WHAT THE ENGINE CAN STILL REACH: installed around a read whose answer
 * goes to a client and nowhere else, so a cluster missing documents answers it
 * with a partial page instead of an error.
 *
 * ```kotlin
 * withContext(AcceptPartial) { store.rawQuery(filters, onEach) }
 * ```
 *
 * The case it exists for is a content node going offline. Its documents have
 * no live searchable replica (redundancy 1, or `searchable-copies` below
 * redundancy while the survivors index theirs), so every engine response
 * comes back below 100% with `non-ideal-state` set until the node returns.
 * Without this every REQ and NIP-50 search refuses for the whole outage
 * ("the response is a PARTIAL answer, not a small one, so it is refused").
 * Under it they are served from the part of the corpus that answered, and
 * every such answer is booked `served` in the engine's `DegradedReads`, so a
 * relay running partial stays visible.
 *
 * WHAT IT COVERS: [NostrSemanticsStore.query], [NostrSemanticsStore.rawQuery]
 * and [NostrSemanticsStore.scoredProfileSearch], every engine read their page
 * makes (the recall, the search expansion's companions and spliced subjects,
 * the `sort:links` rerank). And only a cluster MISSING documents
 * (`non-ideal-state`, or a shortfall with no reason named, over at least one
 * searched document): a match-phase cut the profile did not ask for, a
 * timeout, a node answering with errors, or a response that searched nothing
 * still refuse.
 *
 * THE LENS: a read with an observer resolves it from the cached provider map.
 * When that cache is cold (a restart, a 10040 write) and the cluster refuses
 * the rebuild, the lens comes from the lists it can still search, a pass held
 * for 30 s and never cached as the real one. An observer whose 10040 is not in
 * it is refused (`PartialAnswer.LENS`): "no list" and "a list on the node that
 * is down" read the same, and ranking them as trusting nobody would gate their
 * page empty.
 *
 * WHAT IT DOES NOT: NIP-45 counts (a count over part of the corpus is a wrong
 * number, not a short one), negentropy snapshots, the live gate, and every
 * write path's own reads, which decide by "did the query find it". Those
 * refuse whatever the context says.
 *
 * OPT-IN, and the consumer's call: whether a client gets a partial page or an
 * error is relay policy. A consumer's OWN reads that decide something (does my
 * profile exist, which events does a peer lack) must not run under it, and do
 * not by default, which is why this is a marker the serving path installs
 * rather than a store setting.
 */
data object AcceptPartial : CoroutineContext.Element, CoroutineContext.Key<AcceptPartial> {
    override val key: CoroutineContext.Key<*> get() = this
}
