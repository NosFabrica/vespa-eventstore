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

import java.util.concurrent.atomic.AtomicInteger

/**
 * One Vespa container role's endpoints, handed out round-robin per request.
 *
 * A role is a set of containers serving the same handlers — `<document-api/>`
 * or `<search/>` — and the two may be separate clusters (docs/scaling.md,
 * "Feed and query containers"), so a client keeps one rotation per role
 * rather than one for the whole deployment. Empty [endpoints] fall back to
 * [fallback], the single-URL deployment every default assumes.
 */
internal class EndpointRotation(
    endpoints: List<String>,
    fallback: String,
) {
    /** The endpoints, trailing slashes trimmed so a path can be appended as is. */
    val urls: List<String> = endpoints.ifEmpty { listOf(fallback) }.map { it.trimEnd('/') }

    private val cursor = AtomicInteger()

    /** The endpoint for the next request. */
    fun next(): String = urls[Math.floorMod(cursor.getAndIncrement(), urls.size)]
}
