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
package com.nosfabrica.vespa.eventstore.engine.tuning

/**
 * A setting this library holds ONCE PER PROCESS: a fan-out width, a match-phase
 * depth, a chunk size — values the code reads as a top-level or companion
 * constant from places no store instance reaches (a `mapBounded` deep in the
 * ingest path, a YQL builder that is an `object`).
 *
 * They used to be `val X = System.getenv("VESPA_X")?.toIntOrNull() ?: default`,
 * read at class load and LENIENTLY: a typo kept the default without a word, so
 * vespa-relay re-parsed each one strictly before opening the store, and the two
 * readings were one more thing to keep in step. Now the value is [pinned] by
 * `VespaEventStore.open(tuning = …)`, which parses strictly; a process that
 * pins nothing reads [env] exactly as before ([lenient], else [default]), so
 * `open()` without a tuning behaves as it always did.
 *
 * PINNED ONCE, and a second pin to a DIFFERENT value fails. These are
 * process-wide by construction — two stores in one JVM share them, as they
 * shared the environment variable — so the honest answer to two opens asking
 * for two widths is a refusal, not whichever ran last. Pinning the same value
 * again is a no-op, so a process that opens several stores from one tuning is
 * fine.
 */
class ProcessKnob<T : Any>(
    /** The environment variable read when nothing pinned a value. */
    val env: String,
    /** This build's value when neither a pin nor [env] says otherwise. */
    val default: T,
    /** [env]'s historical, lenient parse: null where it ignored the value. */
    private val lenient: (String) -> T?,
) {
    @Volatile
    private var pinned: T? = null

    /** Read at most once, and only if nothing was pinned before the first read. */
    private val fromEnvironment: T by lazy { System.getenv(env)?.let(lenient) ?: default }

    /** The value in force: the pinned one, else [env]'s lenient reading, else [default]. */
    val value: T get() = pinned ?: fromEnvironment

    /** Fix the value for the rest of the process; see the class KDoc for why a second, different pin throws. */
    @Synchronized
    fun pin(to: T) {
        val held = pinned
        check(held == null || held == to) {
            "$env is process-wide and an earlier open() already set it to $held; this one asks for $to. " +
                "Open every store in one process with the same value."
        }
        pinned = to
    }

    /** Test seam: forget the pin. A test that pins must not leak it into the next test's JVM state. */
    internal fun unpin() {
        pinned = null
    }
}

/**
 * The `:engine` side's process-wide settings (the store keeps its own beside
 * the code that reads them). Each is documented where it is READ — [env] names
 * the variable, the reading constant explains the number.
 */
object ProcessKnobs {
    /** `VESPA_QUERY_FANOUT` — see `engine.async.QUERY_FANOUT`. At least 1. */
    val queryFanout = ProcessKnob("VESPA_QUERY_FANOUT", 4) { it.toIntOrNull()?.coerceAtLeast(1) }

    /** `VESPA_PUT_FANOUT` — see `engine.async.PUT_FANOUT`. At least 1 (the fan-out coerces anything lower). */
    val putFanout = ProcessKnob("VESPA_PUT_FANOUT", 32) { it.toIntOrNull() }

    /** `VESPA_MATCH_PHASE_MAX_HITS` — see `EventYql.MATCH_PHASE_MAX_HITS`; must mirror the SERVING schema. */
    val matchPhaseMaxHits = ProcessKnob("VESPA_MATCH_PHASE_MAX_HITS", 20_000) { it.toIntOrNull()?.coerceAtLeast(1) }
}
