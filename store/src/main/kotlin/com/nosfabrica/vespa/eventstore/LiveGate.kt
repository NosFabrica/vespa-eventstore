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

import com.nosfabrica.vespa.eventstore.engine.text.IndexableChars
import com.nosfabrica.vespa.eventstore.trust.TrustCells
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter

/**
 * THE OBSERVER GATE FOR EVENTS THAT ARRIVE AFTER EOSE.
 *
 * A lensed subscription's stored page is gated in the engine: below-floor
 * authors never leave it. Live events never touch the engine — the relay
 * matches them against the subscription's filters in memory — so without this
 * a signed-in feed dropped an author from its history and then streamed that
 * same author's next note. One gate per subscription, built by
 * [NostrSemanticsStore.liveGate] from the SAME query each filter's stored read
 * compiles to (its floor is `EventYql.gateFloor`, the rule every read path
 * shares), so a live event is delivered when the page would have served it.
 *
 * A RULE VOUCHES ONLY FOR WHAT IT MATCHES, TEXT INCLUDED. Quartz's
 * `Filter.match` ignores `search`, so an in-memory match of a searching filter
 * is every event its NIP-01 part admits — and an UNGATED searching rule
 * (`sort:text`, `include:spam`) beside a gated sibling would then vouch for any
 * event the sibling was there to judge. Each rule with text therefore also
 * checks it against the event's indexed text ([Text]): every term a prefix of
 * some word, every phrase present word for word, no excluded word. That is the
 * engine's exact and prefix tiers; a match it only reaches by typo or n-gram is
 * not streamed live (the next REQ serves it), which is the safe way to be wrong.
 *
 * Two speeds, because live delivery runs on the ingest path and must not wait:
 * [admitsNow] answers from trust already in hand (a hit is a map lookup), and
 * says null when an author's trust has to be read first; [admits] reads it.
 * A caller delivers on `true`, drops on `false`, and on null defers the event
 * to [admits] off the ingest path — never delivers first and checks later.
 */
class LiveGate internal constructor(
    private val rules: List<Rule>,
    private val trust: TrustCells,
    private val textOf: (Event) -> String,
) {
    /**
     * A filter's text, as the live check reads it: lower-cased words. Phrases
     * and exclusions arrive as the PIECES the engine ran them as
     * (`PhraseRuns.pieces`): every piece of a phrase is required, and an
     * exclusion drops an event holding every piece of it — so a live event is
     * judged by the query the page was read with, not by the text as typed.
     */
    internal class Text(
        val terms: List<String>,
        val phrases: List<List<String>>,
        val excluded: List<List<List<String>>>,
    ) {
        fun isEmpty() = terms.isEmpty() && phrases.isEmpty() && excluded.isEmpty()

        fun admits(words: List<String>): Boolean =
            terms.all { t -> words.any { it.startsWith(t) } } &&
                phrases.all { p -> words.windowed(p.size).any { it == p } } &&
                excluded.none { pieces -> pieces.all { x -> words.windowed(x.size).any { it == x } } }

        companion object {
            /** The engine's tokens ([IndexableChars.tokens]): letter/digit runs, and each emoji a word of its own. */
            fun words(text: String?): List<String> = text?.lowercase()?.let(IndexableChars::tokens).orEmpty()
        }
    }

    /**
     * One filter's gate: its [text] (null when it searches nothing), the
     * service whose rank cell its lens reads ([service]; null is an unresolved
     * lens, "trusts nobody") and its [floor], or no floor when it is ungated.
     */
    internal class Rule(
        val filter: Filter,
        val text: Text?,
        val service: String?,
        val floor: Double?,
    )

    private fun Rule.matches(
        event: Event,
        words: Lazy<List<String>>,
    ): Boolean = filter.match(event) && (text == null || text.admits(words.value))

    private fun wordsOf(event: Event) = lazy { Text.words(textOf(event)) }

    /** True/false from trust already known; null when an author must be read first. */
    fun admitsNow(event: Event): Boolean? {
        val words = wordsOf(event)
        var unknown = false
        for (rule in rules) {
            if (!rule.matches(event, words)) continue
            val floor = rule.floor ?: return true
            val score = trust.cached(event.pubKey, rule.service)
            if (score == null) {
                unknown = true
            } else if (score >= floor) {
                return true
            }
        }
        return if (unknown) null else false
    }

    /** The full answer, reading any trust not yet in hand. */
    suspend fun admits(event: Event): Boolean {
        val words = wordsOf(event)
        for (rule in rules) {
            if (!rule.matches(event, words)) continue
            val floor = rule.floor ?: return true
            if (trust.read(event.pubKey, rule.service) >= floor) return true
        }
        return false
    }
}
