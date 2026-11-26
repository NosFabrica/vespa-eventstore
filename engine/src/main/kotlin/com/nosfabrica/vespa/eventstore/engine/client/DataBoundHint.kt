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

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * One bound of the stored data — the newest `created_at` at or before the clock, or the oldest —
 * as one cheap engine read ([read]) last found it. For PLANNING only: a read may start where the
 * data ends, or skip straight to its unwindowed query below where it starts, and either way the
 * page is proven by the engine, so a stale bound (a write after the hint, a backfilled old event)
 * costs a query, never a page.
 *
 * Never read until something [want]s it — a store whose reads never need it never sends the
 * query — then re-read every [ttlMillis]: the first time by the read that asked ([refresh] runs
 * inline, so that read is planned by it), every later time in [scope] while reads go on with the
 * value there is. One refresh at a time; a failed read keeps the old value.
 */
internal class DataBoundHint(
    private val ttlMillis: Long,
    private val scope: CoroutineScope,
    private val read: suspend (nowSecs: Long) -> Long?,
) {
    private val value = AtomicLong(UNKNOWN)
    private val wanted = AtomicBoolean(false)
    private val askedAtMillis = AtomicLong(0L)
    private val refreshing = AtomicBoolean(false)
    private val asked = AtomicBoolean(false)

    /** The bound, or null before the first answer (and on a store the read found empty). */
    fun get(): Long? = value.get().takeIf { it != UNKNOWN }

    /** From now on [refresh] reads the bound. */
    fun want() = wanted.set(true)

    /** Read the bound if it is wanted and older than [ttlMillis]: inline the first time, else in [scope]. */
    suspend fun refresh() {
        if (!wanted.get()) return
        val nowMillis = System.currentTimeMillis()
        if (nowMillis - askedAtMillis.get() < ttlMillis) return
        if (!refreshing.compareAndSet(false, true)) return
        askedAtMillis.set(nowMillis)
        if (asked.compareAndSet(false, true)) {
            fetch(nowMillis / 1000)
        } else {
            try {
                scope.launch { fetch(nowMillis / 1000) }
            } catch (e: Throwable) {
                refreshing.set(false)
                throw e
            }
        }
    }

    private suspend fun fetch(now: Long) {
        try {
            value.set(read(now) ?: UNKNOWN)
        } catch (e: CancellationException) {
            throw e
        } catch (_: Exception) {
            // A hint: the value there is (or none) serves until the next refresh.
        } finally {
            refreshing.set(false)
        }
    }

    private companion object {
        const val UNKNOWN = Long.MIN_VALUE
    }
}
