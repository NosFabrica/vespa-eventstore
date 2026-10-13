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
package com.nosfabrica.vespa.eventstore.trust

import com.nosfabrica.vespa.eventstore.engine.EventIndex
import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.ObserverKey
import com.nosfabrica.vespa.eventstore.engine.query.EventQuery
import com.nosfabrica.vespa.eventstore.mapping.toEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import java.util.concurrent.atomic.AtomicLong

/**
 * WHAT THE STORED 10040s SAY, in the two shapes the trust code reads. The
 * write side asks which SERVICES are named at all — a card by a named service
 * becomes a cell keyed by that service, a card by anyone else is dead storage
 * — and per dimension, so the reconciler can judge a service's projection by
 * the tag it was named for. The read side asks for one observer's LENS: the
 * service their list resolves to per dimension, which is what a query's
 * `user_q` / `followers_q` carry. A user may name DIFFERENT services for the
 * two, which is why the lens is per dimension.
 */
internal data class TrustProviders(
    /** The service keys some 10040 names under `30382:rank`. */
    val rankServices: Set<String>,
    /** The service keys some 10040 names under `30382:followers`. */
    val followerServices: Set<String>,
    /**
     * observer -> the ONE service key per dimension their 10040 resolves to.
     * NIP-85 prescribes no merge across several providers for one metric, and
     * Amethyst models one slot per metric, so the FIRST entry per dimension
     * wins; the rest are still mapped (their cards project) but not read for
     * this observer.
     */
    val lenses: Map<ObserverKey, Lens> = emptyMap(),
) {
    /** One observer's resolved providers; null on a dimension their 10040 does not name. */
    data class Lens(
        val rank: String?,
        val followers: String?,
    )

    fun isEmpty() = rankServices.isEmpty() && followerServices.isEmpty()

    /** [observer]'s lens, or an empty one for an observer with no stored 10040. */
    fun lensOf(observer: String): Lens = lenses[ObserverKey(observer)] ?: NO_LENS

    companion object {
        val NO_LENS = Lens(null, null)
    }

    /** Whether [service] is named for either dimension — its cards can project. */
    fun maps(service: String) = service in rankServices || service in followerServices

    /** Every named service key, either dimension. */
    val services: Set<String> get() = rankServices + followerServices
}

/**
 * Derives [TrustProviders] from every stored kind-10040 — the one place the
 * observer->service link is read: a 30382 is SIGNED by a service key and
 * stored under it; the 10040 is the pointer an observer's query follows to
 * it. A popular provider named by many lists is one set entry and one lens
 * per list, never a copy per observer.
 *
 * [get] is CACHED across a pass — the maps only change on a 10040
 * write/removal, each of which [invalidate]s when it goes through THIS
 * process. One that another process stores (the sync router mirroring a list
 * into the index a relay serves from) reaches no [invalidate] here, so under a
 * shared writer topology the owner also calls [refresh] on an interval —
 * `NostrSemanticsStore`'s provider refresher. Without it the pass was as old
 * as the last 10040 this process wrote itself: on staging (2026-09-26) 24 of 47
 * recent observers resolved no lens and got EMPTY ranked pages, and Trusted
 * Lists their stored 10040 delegated did not unpack (#145). The rebuild runs UNLOCKED (an
 * observer-carrying search asks for the read-side gate with no lock at all),
 * so a 10040 write can land while a pass is reading the corpus; a pass that
 * then stored its result would cache a map missing that write, and nothing
 * would drop it until the next 10040 — a query under the new list would
 * resolve to no lens, and the write side would judge its services already
 * named. So every [invalidate] bumps a generation, and a pass is stored only
 * if the generation it started under is still current.
 */
internal class ProviderMap(
    private val inner: EventIndex,
    private val nowSecs: () -> Long,
) {
    /**
     * Both projections of one pass. The trust side wants the named services
     * per tensor and each observer's lens; the search gate wants observer ->
     * signers per kind, to decide which declarations a search may unpack
     * ([Delegations]). Same query, same parse, same invalidation — deriving
     * them separately would be a second place the observer/signer link is
     * resolved, and the two would drift the first time a Map shape changed.
     */
    private class Pass(
        val trust: TrustProviders,
        val delegations: Delegations,
        /** When the query behind it was ISSUED (not answered) — what [ageSecs] measures from. */
        val builtAtSecs: Long,
        /** Build order: a pass never replaces one whose query was issued after its own. */
        val seq: Long,
    )

    @Volatile private var cached: Pass? = null

    /** Bumped by every [invalidate]; a pass built under an older generation is not cached. */
    private val generation = AtomicLong()

    /** Issued to every build, in order; see [Pass.seq]. */
    private val builds = AtomicLong()

    /**
     * Makes "is the generation still current, and is nothing newer cached"
     * and the store ONE step, against [invalidate] and against another build.
     * Unlocked, a build could pass the generation check, lose the CPU to a
     * 10040 write's invalidate, and then cache a pass missing that write —
     * the exact outcome the generation exists to prevent, and nothing would
     * drop it before the next tick. Held for two field writes, never across
     * the query.
     */
    private val swapLock = Any()

    /**
     * SIGNERS WHOSE CARDS THIS PROCESS SKIPPED because no pass it held named
     * them ([TrustRecompute.applyCards] drops such a card as dead storage).
     * When a later pass names one — a 10040 another process stored, which
     * this process could not see — those cards need a WALK here, or they stay
     * unprojected: the other process's walk ran when it named the service, and
     * cannot reach cards stored after it. [takeSkippedNowNamed] hands them to
     * the refresher.
     *
     * Keyed by what was actually SKIPPED, not by "a service this pass names
     * that the last did not": that diff re-walked, in every process, every
     * service any OTHER process named — O(its cards) each, 279,594 for the
     * largest — whether or not a single card had been skipped here; and it had
     * to be seeded from a first pass, which a boot under a not-yet-serving
     * engine got wrong. A signer here was skipped here, so its walk is owed.
     *
     * Bounded by [SKIPPED_CAP] distinct signers — a mirror ingesting cards by
     * kind meets many providers no list names. Past the cap a skip is not
     * remembered and its cards wait for a reconcile; [skippedDropped] counts
     * them so that is visible rather than silent.
     */
    private val skipped = LinkedHashSet<String>()

    /** Skips not remembered because [skipped] was full. */
    @Volatile var skippedDropped: Long = 0
        private set

    /** Guards [skipped] and [skippedDropped]. */
    private val skippedLock = Any()

    /**
     * The maps, rebuilt once per pass. Already-expired 10040s (NIP-40) are
     * excluded, matching every read path — a list not served as a record must
     * not keep attributing (its cells stand until the expiry sweep fires the
     * projection's react).
     *
     * An EMPTY result is never cached. A relay with no 10040s and one whose
     * engine has not finished serving its corpus return the same empty list, and
     * caching it makes the second permanent: [invalidate] fires only on a 10040
     * WRITE, and dedup drops an already-held 10040 before any write (observed:
     * 271 queryable 10040s while ten minutes of reconcile retries read the same
     * cached emptiness). Not caching costs one small query per pass on a
     * genuinely providerless relay.
     */
    suspend fun get(): TrustProviders = pass().trust

    /**
     * The read-time gate, off the same pass. See [Delegations] for what a Map
     * entry's kind means and why a named `3039x:<name>` entry is not admitted.
     */
    suspend fun delegations(): Delegations = pass().delegations

    private suspend fun pass(): Pass = cached ?: build()

    /**
     * REBUILD NOW — the refresher's tick. Readers keep the previous pass until
     * this one lands; only a cold cache ever makes a reader wait. A rebuild
     * that THROWS leaves the previous pass in place (the caller counts the
     * failure), and one that reads NO documents does too — the
     * empty-never-cached rule below, applied to a refresh: a relay that held
     * 418 lists a minute ago and reads none now is one whose engine is not
     * serving them, not one whose readers all deleted theirs. The price, a
     * known limit: another process deleting the LAST stored 10040 is not seen
     * until this process invalidates on its own.
     *
     * A no-op while nothing is cached: the next reader builds a fresh pass
     * anyway, and a process that never reads trust never pays for one.
     */
    suspend fun refresh() {
        if (cached != null) build()
    }

    /** Seconds since the cached pass was issued, or null while nothing is cached — the staleness gauge. */
    fun ageSecs(): Long? = cached?.let { nowSecs() - it.builtAtSecs }

    /** [signers]' cards were just skipped as unmapped — see [skipped]. One lock per batch, not per card. */
    fun noteSkipped(signers: Collection<String>) {
        if (signers.isEmpty()) return
        synchronized(skippedLock) {
            for (signer in signers) {
                if (signer in skipped) continue
                if (skipped.size >= SKIPPED_CAP) skippedDropped++ else skipped += signer
            }
        }
    }

    /**
     * A walk of [services] was just queued by a local 10040 write; the walk
     * reads every stored card, so their skips are covered. Called only once
     * the write has STORED — a list that lost supersession queues nothing, so
     * its services' skips must stay owed.
     */
    fun walkQueued(services: Collection<String>) {
        if (services.isEmpty()) return
        synchronized(skippedLock) { skipped.removeAll(services.toSet()) }
    }

    /** Whether [takeSkippedNowNamed] would return anything — a peek, so a quiet tick takes no lock. */
    fun hasSkippedNowNamed(): Boolean {
        val named = cached?.trust ?: return false
        return synchronized(skippedLock) { skipped.any(named::maps) }
    }

    /**
     * The skipped signers the CACHED pass now names, removed from the set: the
     * caller queues their walks, and [giveBack]s any it could not queue.
     */
    fun takeSkippedNowNamed(): Set<String> {
        val named = cached?.trust ?: return emptySet()
        return synchronized(skippedLock) {
            val taken = skipped.filterTo(LinkedHashSet(), named::maps)
            skipped.removeAll(taken)
            taken
        }
    }

    /** [takeSkippedNowNamed]'s signers back, for a caller whose walk was never queued. */
    fun giveBack(signers: Collection<String>) = noteSkipped(signers)

    private suspend fun build(): Pass {
        val startedUnder = generation.get()
        val seq = builds.incrementAndGet()
        val issuedAt = nowSecs()
        // `complete`: a map built from SOME of the 10040s names none of the
        // services the missing lists name, so their cards would not project
        // and their observers would resolve to no lens — the same wrong
        // answer as a short card fetch, one level up.
        val docs = inner.search(EventQuery(kinds = listOf(TrustProviderListEvent.KIND), notExpiredAt = issuedAt, complete = true))
        // ONE parse for both projections. Each doc costs a tags parse and an
        // `EventFactory` dispatch, and this rebuilds on every 10040 write, so
        // handing each side the documents to decode itself would double the
        // cost of the pass the KDoc above promises is shared.
        val maps = docs.mapNotNull { it.toEvent() as? TrustProviderListEvent }
        val fresh = Pass(providersIn(maps), Delegations.delegationsOf(maps), issuedAt, seq)
        // Emptiness is judged on the DOCUMENTS, not on either projection: the
        // ambiguity this rule records is "no 10040s" versus "the engine has not
        // finished serving them", and only the query's own answer distinguishes
        // those. Judging it on the trust half instead would also refuse to cache
        // a relay whose Maps carry nothing but bare-kind Trusted List entries —
        // a real shape, and one with no rank or followers provider in it.
        if (docs.isEmpty()) return cached ?: fresh
        synchronized(swapLock) {
            // Two builds under one generation (a tick and a cold read, or a tick
            // and a manual refresh) can finish in either order; the one whose
            // query was issued LATER is the one to keep.
            val current = cached
            if (generation.get() == startedUnder && (current == null || current.seq < seq)) cached = fresh
        }
        return fresh
    }

    /** Drop the cache; the next [get] rebuilds. Call after any 10040 write/remove. */
    fun invalidate() {
        synchronized(swapLock) {
            generation.incrementAndGet()
            cached = null
        }
    }

    companion object {
        /** The named services per dimension, and every lens, from [listDocs]' typed entries. */
        fun providersOf(listDocs: List<EventDoc>): TrustProviders = providersIn(listDocs.mapNotNull { it.toEvent() as? TrustProviderListEvent })

        /** The same, off Maps already decoded — see [pass] for why that matters. */
        fun providersIn(maps: List<TrustProviderListEvent>): TrustProviders {
            val rank = LinkedHashSet<String>()
            val followers = LinkedHashSet<String>()
            val lenses = LinkedHashMap<ObserverKey, TrustProviders.Lens>()
            maps
                .forEach { list ->
                    var lens = lenses[ObserverKey(list.pubKey)] ?: TrustProviders.NO_LENS
                    list.serviceProviders().forEach { entry ->
                        when (entry.service) {
                            ProviderTypes.rank -> {
                                rank += entry.pubkey
                                if (lens.rank == null) lens = lens.copy(rank = entry.pubkey)
                            }

                            ProviderTypes.followerCount -> {
                                followers += entry.pubkey
                                if (lens.followers == null) lens = lens.copy(followers = entry.pubkey)
                            }
                        }
                    }
                    if (lens != TrustProviders.NO_LENS) lenses[ObserverKey(list.pubKey)] = lens
                }
            return TrustProviders(rank, followers, lenses)
        }

        /**
         * Most unmapped signers remembered at once — a few MB of hex keys.
         * Unmapped cards are the bulk of a by-kind mirror's card ingest, but
         * from far fewer distinct providers than this.
         */
        const val SKIPPED_CAP = 50_000

        /** The distinct service keys (either dimension) named across a batch of 10040 lists. */
        fun trustServicesOf(listDocs: List<EventDoc>): List<String> = providersOf(listDocs).services.toList()
    }
}
