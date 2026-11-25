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

import com.nosfabrica.vespa.eventstore.engine.client.EngineRefusesWrites

/**
 * Whether [this], or what it wraps, is the store saying its engine is refusing EVERY write right
 * now: Vespa blocks external feed for the whole content cluster once a node passes its disk or
 * memory limit, until an operator frees space. A failure of the engine, not of any event in the
 * write, so a caller isolating a bad event by retrying smaller parts of a batch gains nothing, and
 * its publishers should hear "try again later", not "your event was refused".
 *
 * The facade's name for it, so a consumer need not reach into the engine. Up to eight causes
 * deep: a write path may wrap the engine's failure in its own.
 */
fun Throwable.isStoreRefusingWrites(): Boolean = generateSequence(this) { it.cause }.take(8).any { it is EngineRefusesWrites }
