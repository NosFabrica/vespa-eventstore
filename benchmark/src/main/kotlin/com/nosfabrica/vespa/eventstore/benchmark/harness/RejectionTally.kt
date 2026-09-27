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
package com.nosfabrica.vespa.eventstore.benchmark.harness

import com.vitorpamplona.quartz.nip01Core.store.RejectionReason

/**
 * The key a loader or probe tallies a rejection [reason] under: its NIP-01
 * prefix (`duplicate`, `blocked`, …) — bounded, since the tail can name the
 * event — EXCEPT a superseded replaceable, which keys as `superseded`.
 *
 * Since Quartz 28bf170f92 a stale version is `duplicate:`-prefixed like an id
 * re-offer (so a relay acks it `OK true`), and the prefix alone would fold the
 * two together. They mean different things to someone reading a load report:
 * a capture that re-offers what is stored, versus one carrying older versions
 * of what is stored — which TrustProbe provokes on purpose.
 */
fun tallyKey(reason: String): String = if (reason == RejectionReason.SUPERSEDED) "superseded" else reason.substringBefore(':')
