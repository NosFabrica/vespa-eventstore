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

/**
 * One hit with the engine's own relevance beside it.
 *
 * The carrier for the one question a recall cannot answer after the fact: WHERE
 * a hit belongs relative to a hit from a DIFFERENT query. Within one query the
 * engine's order is the answer and the score is redundant; across two, the
 * order is two orders and there is nothing to merge on unless the number comes
 * back with the hit.
 *
 * `score` is null when the engine does not rank — [InMemoryEventIndex] sorts by
 * recency and has no relevance to give, and saying so is the point: a fabricated
 * constant would make a merge that reads as ranked and is arbitrary. A caller
 * that sees a null merges some other way (the store falls back to recency,
 * which is the order those hits are already in).
 *
 * Scores compare only within ONE rank profile. Two queries scored by different
 * profiles produce two different scales, and the store checks that before it
 * merges — see NostrSemanticsStore.recallOrdered.
 *
 * Distinct from [ScoredHit], which is the inspector's surface: that one is
 * EventDoc-shaped and carries the match TIER for explaining a ranking to a
 * human. This is the recall path's, generic over what a projection returned.
 */
data class Ranked<T>(
    val hit: T,
    val score: Double?,
)
