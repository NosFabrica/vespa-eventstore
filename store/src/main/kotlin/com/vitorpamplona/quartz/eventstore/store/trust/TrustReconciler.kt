/*
 * Copyright (c) 2026 Vitor Pamplona
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
package com.vitorpamplona.quartz.eventstore.store.trust

import com.vitorpamplona.quartz.eventstore.vespa.client.EventIndex
import com.vitorpamplona.quartz.eventstore.vespa.client.ReputationIndex
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent

/**
 * The repair tool for a projection no write can reach — [TrustProjection]
 * derives on WRITE, and a trigger only fires once, so a corpus stored before
 * its provider lists stays unprojected with nothing left to trip. This class
 * finds and re-derives that drift: [reconcile] per affected service (worth
 * running at startup), [rebuildAll] as the operator's hammer.
 */
class TrustReconciler internal constructor(
    private val index: EventIndex,
    private val reputations: ReputationIndex,
    private val recompute: TrustRecompute,
) {
    /** What [reconcile] found: services examined, and the ones it had to re-derive. */
    data class Reconciliation(
        val services: Int,
        val rebuilt: List<String>,
    ) {
        fun isClean() = rebuilt.isEmpty()
    }

    /**
     * Re-derive the services whose scores are not projected under the observer
     * currently mapped to them.
     *
     * ## Why this is needed at all
     *
     * Derivation happens on WRITE: [TrustProjection.putAll] projects the cards
     * in the batch, and a 10040 arriving re-walks the services it names. Both
     * are triggers, and a trigger only fires once. Dedup rejects an event the
     * store already holds BEFORE the projection sees it, so once a corpus is
     * stored neither trigger can fire again — a card skipped because its
     * service had no 10040 yet stays unprojected for as long as both events
     * remain in the store.
     *
     * A mirror hits this as a matter of course: scores outnumber provider lists
     * by four orders of magnitude and arrive first, and the run that finally
     * writes the 10040 may be one in which every score is already a duplicate.
     * The result is a projection that is empty, correct-looking and unable to
     * repair itself — every ranked search returns nothing, with no error anywhere.
     *
     * ## What it checks
     *
     * A service is projected when its subjects carry a cell for ITS observer. So
     * for each mapped service this samples a few of its cards and asks whether
     * any of their subjects has that observer's cell. Checking the observer
     * rather than mere existence is what also catches a RE-MAPPED service: its
     * subjects have docs, but the cells belong to the previous observer.
     *
     * Sampling, not counting, because the alternative is reading every card. A
     * service whose sample happens to land on retracted subjects is re-derived
     * needlessly, which costs one walk and is idempotent. The opposite error —
     * calling an unprojected service clean — would need a sampled subject to be
     * projected while the rest are not, which is not the shape this failure takes.
     *
     * ## Progress
     *
     * [onProgress] reports `(inspected, total, rebuilt, derivedInService)` before
     * each service, and again inside a rebuild as its subjects are re-derived.
     * This runs at startup over every provider list the store holds, and one
     * service can cost a full walk of six figures of documents — minutes during
     * which ranked search does not work and the caller could say only that it had
     * begun. `total` is known up front, so a caller can show a real fraction
     * rather than a spinner.
     */
    suspend fun reconcile(
        samplesPerService: Int = DEFAULT_RECONCILE_SAMPLES,
        onProgress: ((inspected: Int, total: Int, rebuilt: Int, derivedInService: Int) -> Unit)? = null,
    ): Reconciliation {
        val serviceToObserver = recompute.providerMap()
        if (serviceToObserver.isEmpty()) return Reconciliation(0, emptyList())

        val rebuilt = mutableListOf<String>()
        var examined = 0
        var inspected = 0
        val total = serviceToObserver.size
        for ((service, observer) in serviceToObserver) {
            // Before, not after: the expensive part of a service is the walk that
            // may follow, so a caller reporting "inspected 47 of 124" after the
            // fact would sit silent through exactly the slow one.
            onProgress?.invoke(inspected, total, rebuilt.size, 0)
            inspected++
            val sample =
                index.search(
                    EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service), limit = samplesPerService),
                )
            // A service that has published nothing we hold has nothing to project.
            if (sample.isEmpty()) continue
            examined++
            val projected =
                sample.any { card ->
                    val subject = subjectOf(card) ?: return@any false
                    reputations.get(subject)?.influenceScores?.containsKey(observer) == true
                }
            if (!projected) {
                // The expensive case, and the reason a caller needs progress at
                // all: one provider can hold six figures of scores, so this walk
                // is most of the time reconcile takes. Reporting only between
                // services would sit still through exactly the slow part.
                recompute.recomputeWalk(EventQuery(kinds = listOf(ContactCardEvent.KIND), authors = listOf(service))) { derived ->
                    onProgress?.invoke(inspected - 1, total, rebuilt.size, derived)
                }
                rebuilt += service
            }
        }
        onProgress?.invoke(inspected, total, rebuilt.size, 0)
        return Reconciliation(examined, rebuilt)
    }

    /**
     * Re-derive every parent doc from scratch (bootstrap over an existing
     * index). Bounded only by the corpus — [reconcile] does the same repair per
     * affected service and normally finds nothing to do.
     */
    suspend fun rebuildAll() = recompute.recomputeWalk(EventQuery(kinds = listOf(ContactCardEvent.KIND)))

    private companion object {
        // Cards sampled per service by [reconcile]. The question it answers is
        // "did this service's scores get projected at all", and the failure is
        // all-or-nothing per service, so a handful settles it; the cost is one
        // small query plus that many key lookups per mapped service.
        const val DEFAULT_RECONCILE_SAMPLES = 3
    }
}
