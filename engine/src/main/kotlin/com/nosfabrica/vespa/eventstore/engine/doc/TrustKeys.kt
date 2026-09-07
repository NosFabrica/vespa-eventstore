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
package com.nosfabrica.vespa.eventstore.engine.doc

import com.vitorpamplona.quartz.nip01Core.core.HexKey

/**
 * THE PUBKEY A REPUTATION CELL IS KEYED BY: the service that SIGNED the
 * kind-30382, never the observer who reads it.
 *
 * The two are both 64-hex and were both `HexKey`, which is
 * `typealias HexKey = String` — a label, not a type. Getting them the wrong
 * way round is what the service-keyed migration existed to repair, and the
 * confusion outlived it: [ReputationDoc.indexFields] was still destructuring
 * its own cells as `(observer, rank)` four lines under a doc comment saying
 * they key by service. A value class costs nothing at runtime and makes that
 * a compile error.
 */
@JvmInline
value class ServiceKey(
    val hex: HexKey,
)

/**
 * THE PUBKEY WHOSE KIND-10040 SELECTS a service per dimension — the reader's
 * lens, resolved to a [ServiceKey] at query time. Never a cell key itself.
 */
@JvmInline
value class ObserverKey(
    val hex: HexKey,
)

/**
 * Reputation cells keyed by the SIGNING SERVICE, built from plain hex —
 * `serviceCells(svc to 40)` says what `mapOf(svc to 40)` only implied, and the
 * implication was wrong often enough to need a migration.
 */
fun <V> serviceCells(vararg cells: Pair<HexKey, V>): Map<ServiceKey, V> = cells.associate { ServiceKey(it.first) to it.second }
