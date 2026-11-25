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
package com.nosfabrica.vespa.eventstore.engine.client

/**
 * The engine refused a write because it is refusing EVERY write: Vespa blocks external feed for
 * the whole content cluster once a node passes its disk or memory limit (`resource-limits`), and
 * answers each operation `INSUFFICIENT_STORAGE` until an operator frees space.
 *
 * A failure of the engine, not of any document in the write, so a caller isolating a bad document
 * by retrying smaller parts of a batch (vespa-relay's ingest bisection) gains nothing from it:
 * every part fails the same way. Thrown at once by the batch write path ([VespaBatchFeed]), not
 * after re-putting each document through the feed client, whose retries are what made a refused
 * batch slow.
 */
class EngineRefusesWrites(
    message: String,
) : IllegalStateException(message)
