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
 * One hit with the engine's own relevance beside it — what lets a caller MERGE
 * the hits of two different queries into one order. Within a single query the
 * engine's order already is the answer.
 *
 * `score` is null when the engine does not rank ([InMemoryEventIndex] sorts by
 * recency): a fabricated constant would make a merge that reads as ranked and is
 * arbitrary, so callers seeing a null merge some other way (the store falls back
 * to recency, which those hits are already in).
 *
 * Scores compare only within ONE rank profile — two profiles are two scales, and
 * the store checks that before merging (NostrSemanticsStore.recallOrdered).
 *
 * Distinct from [ScoredHit], the inspector's EventDoc-shaped surface that also
 * carries the match tier.
 */
data class Ranked<T>(
    val hit: T,
    val score: Double?,
)
