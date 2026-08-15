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
package com.nosfabrica.vespa.eventstore.engine.query

import com.nosfabrica.vespa.eventstore.engine.NearText

/**
 * Per-word recall over the schema's search fields and match ladder — the
 * drift-prone half of [EventYql], isolated here; MockVespaEngine's parser
 * guards against drift.
 *
 * One match group per query word, AND'd: EVERY word must land somewhere on
 * the doc. Within its group a word matches loosely (any field, through
 * exact/prefix/fuzzy/trigram alike); ranking sorts the full-coverage results
 * out. Two synthetic groups keep concatenated handles reachable: a joined
 * variant for 2+ words ("John Carvalho" finds @johncarvalho), OR'd against
 * the whole conjunction since it satisfies every word at once; and
 * adjacent-pair concatenations for 3+ words, each standing in for its two words.
 *
 * Injection safety: words go out-of-band as query parameters, never inlined —
 * @w0..@wN AS TYPED (exact clauses; index fields fold linguistically) and
 * @f0..@fN [NearText.foldWord]-folded (near clauses; ATTRIBUTE fields match
 * raw bytes, so the fold must match the feed's or "jose" never reaches
 * "josé"). Trigram literals are filtered to alphanumerics — safe to embed.
 *
 * PREFIX / FUZZY must be DIRECT terms against ATTRIBUTE fields (*_parts /
 * *_tokens), never annotations on `userInput()`: userInput silently DROPS
 * prefix/fuzzy annotations (a no-op, no error), and the direct form against
 * an `index` field is rejected outright (HTTP 400). The INDEX fields stay
 * exact-only; their prefix reach lives on the attribute siblings
 * ([PREFIX_ONLY_FIELDS]).
 */
internal object FuzzyWordGroup {
    /**
     * The attribute fields prefix AND fuzzy terms match against (fed by
     * NearText). Both granularities are load-bearing and both are still fed —
     * parts splits at every word start ("meme" finds "BitcoinMemeTreasury"),
     * tokens keeps whole tokens plus joined variants ("vitorp" prefixes
     * "vitorpamplona") — they just share one column per tier
     * ([NearText.mergeNear]).
     */
    val NEAR_FIELDS = listOf("name_near", "search_primary_near")

    /**
     * Prefix-ONLY attribute fields: hashtag/summary tokens ("bitco" ->
     * #bitcoin) and the identity/affiliation segments of nip05/lud16/website/
     * about. The affil column keeps recall CONTINUOUS while a name is being
     * typed — docs the finished word reaches must not vanish for its prefixes
     * (SearchPrefixLadderIT). No fuzzy on either: this text builds a much
     * larger dictionary than names, and a typo'd hashtag or domain isn't
     * worth the walk. Scored by the schema's WEAK tier, not the near tier.
     */
    val PREFIX_ONLY_FIELDS = listOf("search_secondary_tokens", "affil_tokens")

    /** Every attribute field the near/weak clauses can reference — the client's compatibility net matches 400s against these names. */
    val ALL_NEAR_FIELDS = NEAR_FIELDS + PREFIX_ONLY_FIELDS

    /**
     * Hard ceiling on typos: a hit needing more edits is not matched at all.
     * Vespa's maxEditDistance enforces it at MATCH time, so an over-budget doc
     * never enters the candidate set — it is not merely ranked low.
     */
    const val MAX_TYPO_EDITS = 3

    /**
     * Minimum word length for a prefix clause: 1-2 Latin characters would
     * sweep a large corpus slice for no precision gain, and 3 matches the
     * trigram floor.
     */
    const val MIN_PREFIX_LEN = 3

    /**
     * ...but 3 is a LATIN heuristic: a 2-character CJK query ("中村") is as
     * specific as 5-6 Latin characters. Any word with a non-ASCII character
     * gets the lower floor; those dictionaries are sparse enough to stay cheap.
     */
    const val MIN_PREFIX_LEN_NON_ASCII = 2

    /**
     * Minimum AND'd trigrams for the NAME-side infix nets (word >= 4 chars).
     * AND-of-trigrams is a near-substring test — "dell" (del ∧ ell) reaches
     * ODELL without the old OR net's noise. 2 keeps "dell"-length infixes
     * reachable; hits score in the WEAK tier, under every anchored match.
     */
    const val MIN_AND_GRAMS_NAME = 2

    /**
     * Minimum AND'd trigrams for the long-TEXT nets (word >= 5 chars): long
     * fields hold far more trigrams, so fewer degenerates into a bare
     * substring test. Short words still reach those fields via their exact clause.
     */
    const val MIN_AND_GRAMS_TEXT = 3

    /**
     * Minimum trigrams for a PHRASE gram clause ([PHRASE_GRAM_FIELDS]) — two, so
     * a 4-character word qualifies.
     *
     * Lower than [MIN_AND_GRAMS_TEXT] because the two floors guard different
     * things. ANDing few trigrams degenerates into a bare substring test, so its
     * floor buys PRECISION. A phrase is an exact-substring test at any length —
     * two trigrams as a phrase is still "these six characters, in this order" —
     * so nothing degenerates and the only remaining concern is SELECTIVITY. Two
     * is where the posting-list walk stays worth it; a single trigram matches so
     * much of a body corpus that it is not a query, it is a scan.
     *
     * VERIFIED on Vespa 8 (2026-08-15): `phrase("bit","itc")` — the 4-character
     * word "bitc" — matches a document reading "Bitcoin fixes this". That is the
     * as-you-type keystroke the AND-net's 5-character floor could never serve.
     */
    const val MIN_PHRASE_GRAMS = 2

    /**
     * The per-word groups AND'd into one parenthesized clause, filling
     * [params] with the out-of-band words.
     *
     * Each adjacent-pair concatenation covers exactly ITS two words, so it is
     * OR'd into both words' requirements and nowhere else. The joined variant
     * covers EVERY word at once, so it hoists out of the conjunction and is
     * emitted ONCE — duplicates would inflate matchCount (the exact tier's
     * text score). The pair groups' two-way ride is the accepted residual:
     * bounded, since synthetic groups are exact+prefix only, the shape needs
     * 3+ words, and only docs matching the concatenation see it.
     */
    fun clause(
        words: List<String>,
        params: MutableMap<String, String>,
        nearFields: Boolean = true,
        bodyGram: Boolean = true,
    ): String {
        val own =
            words.mapIndexed { i, word ->
                params["w$i"] = word
                wordGroup("w$i", word, params, synthetic = false, nearFields = nearFields, bodyGram = bodyGram)
            }
        if (words.size == 1) return "(${own[0]})"
        val coverers = List(words.size) { ArrayList<String>() }
        if (words.size >= 3) {
            for (i in 0 until words.size - 1) {
                val pair = words[i] + words[i + 1]
                params["wp$i"] = pair
                val group = wordGroup("wp$i", pair, params, synthetic = true, nearFields = nearFields, bodyGram = bodyGram)
                coverers[i] += group
                coverers[i + 1] += group
            }
        }
        val required =
            words.indices.joinToString(" and ") { i ->
                if (coverers[i].isEmpty()) own[i] else "(${(listOf(own[i]) + coverers[i]).joinToString(" or ")})"
            }
        val joined = words.joinToString("")
        params["wj"] = joined
        val joinedGroup = wordGroup("wj", joined, params, synthetic = true, nearFields = nearFields, bodyGram = bodyGram)
        return "(($required) or $joinedGroup)"
    }

    /** True when the shortest word is short enough to lean harder on the trigram net (drives query(w_gram)). */
    fun leansOnGrams(words: List<String>): Boolean = words.minOf { it.length } <= 3

    /**
     * One word's match clauses: exact per search field, direct prefix/fuzzy
     * against the near attributes, the prefix-only clause, the AND-gram nets.
     *
     * [synthetic] marks the concatenations built in [clause]: exact + prefix
     * but NO fuzzy (a 20+ char concatenation would draw the top typo budget —
     * the most expensive matcher — for a token nobody typed) and no trigrams
     * (the concatenation's grams are a superset of the words' own: noise, no
     * reach).
     *
     * [nearFields] off drops every clause referencing the near/weak attribute
     * fields — the compatibility demotion for a schema that predates them,
     * where any reference is HTTP 400 (see SchemaFallbacks.withNearFallback).
     * [bodyGram] off is the same demotion for [PHRASE_GRAM_FIELDS], which
     * shipped later still. The AND gram nets are NOT gated: those *_gram fields
     * exist on every deployed schema.
     */
    private fun wordGroup(
        name: String,
        literal: String,
        params: MutableMap<String, String>,
        synthetic: Boolean,
        nearFields: Boolean,
        bodyGram: Boolean,
    ): String {
        val clauses = ArrayList<String>()
        for (field in SEARCH_FIELDS) clauses += exactClause(field, "@$name", roleOf(field))
        if (nearFields) {
            // The folded twin rides out-of-band too. Floors and budgets use
            // the FOLDED form — the string the attribute dictionaries hold.
            val folded = NearText.foldWord(literal)
            params["f$name"] = folded
            clauses += nearClauses("@f$name", folded, allowFuzzy = !synthetic)
        }
        if (!synthetic) {
            for (gramField in INFIX_GRAM_FIELDS) {
                andGramClause(literal, gramField, MIN_AND_GRAMS_NAME)?.let { clauses += it }
            }
            for (gramField in TEXT_GRAM_FIELDS) {
                andGramClause(literal, gramField, MIN_AND_GRAMS_TEXT)?.let { clauses += it }
            }
            if (bodyGram) {
                for (gramField in PHRASE_GRAM_FIELDS) {
                    phraseGramClause(literal, gramField)?.let { clauses += it }
                }
            }
        }
        return "(${clauses.joinToString(" or ")})"
    }

    /**
     * The EXACT clause for one (field, word) via userInput() — feeds
     * matchCount(field), the schema's exact tier; labels feed
     * itemRawScore(mtch_*). Prefix/fuzzy are per-word, not per-field, and go
     * to the merged attribute fields via [nearClauses].
     */
    private fun exactClause(
        field: String,
        param: String,
        role: Role,
    ): String {
        val label =
            when (role) {
                Role.PRIMARY -> ",label:\"mtch_exact\""
                Role.AFFILIATION -> ",label:\"mtch_affil\""
                Role.RECALL -> ""
            }
        return "({defaultIndex:\"$field\"$label}userInput($param))"
    }

    /**
     * The direct prefix + fuzzy terms for one FOLDED word, emitted ONCE per
     * word (the attribute fields already merge their sources). These feed the
     * schema's near tier — how an exact hit still ranks above a prefix or
     * typo hit. prefixLength:2 bounds the fuzzy matcher's dictionary walk.
     * Only the TOP edit budget is emitted: maxEditDistance:N subsumes every
     * smaller N, so per-tier clauses would be duplicate matching work.
     */
    private fun nearClauses(
        param: String,
        folded: String,
        allowFuzzy: Boolean,
    ): List<String> {
        val clauses = ArrayList<String>()
        if (folded.length >= minPrefixLen(folded)) {
            for (field in NEAR_FIELDS) clauses += "($field contains ({prefix:true}$param))"
            for (field in PREFIX_ONLY_FIELDS) clauses += "($field contains ({prefix:true}$param))"
        }
        val edits = if (allowFuzzy) wordMaxEdits(folded) else 0
        if (edits > 0) {
            for (field in NEAR_FIELDS) clauses += "($field contains ({maxEditDistance:$edits,prefixLength:2}fuzzy($param)))"
        }
        return clauses
    }

    private fun minPrefixLen(word: String): Int = if (word.all { it.code < 128 }) MIN_PREFIX_LEN else MIN_PREFIX_LEN_NON_ASCII

    /**
     * AND of the word's trigrams against a gram field — every trigram must be
     * present, a bounded near-substring test. [minGrams] separates the tight
     * name-side infix nets ([MIN_AND_GRAMS_NAME]) from the long-text nets
     * ([MIN_AND_GRAMS_TEXT]); below the floor no clause is emitted at all.
     */
    private fun andGramClause(
        word: String,
        gramField: String,
        minGrams: Int,
    ): String? {
        // Lowercase: the *_gram fields are lowercase-indexed — uppercased
        // trigrams from "Vitor" would silently never match.
        val grams = trigrams(word.lowercase())
        if (grams.size < minGrams) return null
        return grams.joinToString(" and ", prefix = "(", postfix = ")") { "$gramField contains \"$it\"" }
    }

    /**
     * A word's trigrams as a PHRASE against a gram field — an exact-substring
     * test, because Vespa's gram tokenizer emits trigrams at CONSECUTIVE
     * positions and a phrase requires them consecutively.
     *
     * This is the whole difference between this net and [andGramClause], and it
     * is a correctness difference, not a tuning one. ANDing the trigrams
     * independently discards position and asks only whether each appears
     * somewhere in the field, which on a long body degenerates: VERIFIED on
     * Vespa 8 (2026-08-15), the AND form of "vitor" matched a document reading
     * "Take a vitamin and open the editor" (`vit` from vitamin, `ito`/`tor` from
     * editor) while the phrase form matched only the document that contains the
     * word. Measured on bodies not containing the query word, the AND net's
     * false-positive rate climbs 0.5% -> 4.0% as the body grows 100 -> 2000
     * words (5-character words); the phrase form measures 0.0% at every length.
     *
     * BAILS OUT rather than dropping a trigram. [trigrams] filters non-alnum
     * grams, which is harmless for an AND (fewer conjuncts, looser) but wrong
     * for a phrase: dropping a middle gram makes the survivors non-adjacent, so
     * the clause demands an adjacency the document cannot have and silently
     * matches nothing. A word with punctuation inside it therefore gets no
     * phrase clause at all and rides its exact clause, which is what tokenized
     * the document that way in the first place.
     */
    private fun phraseGramClause(
        word: String,
        gramField: String,
    ): String? {
        val lower = word.lowercase()
        val all = (0..lower.length - 3).map { lower.substring(it, it + 3) }
        if (all.size < MIN_PHRASE_GRAMS || all.any { gram -> !gram.all(Char::isLetterOrDigit) }) return null
        return all.joinToString(", ", prefix = "($gramField contains phrase(", postfix = "))") { "\"$it\"" }
    }

    /** Alphanumeric-only trigrams — safe to embed in YQL without escaping. */
    private fun trigrams(word: String): List<String> =
        (0..word.length - 3)
            .map { word.substring(it, it + 3) }
            .filter { gram -> gram.all(Char::isLetterOrDigit) }

    /**
     * Per-word typo budget, capped at [MAX_TYPO_EDITS] and length-gated: an
     * edit budget is only meaningful as a FRACTION of the word (~22-25% per
     * tier, where Meilisearch sits) — 3 edits on a 6-letter word matches a
     * different word, not a typo. Does NOT govern prefix matching: an
     * unfinished word is not typos; prefix is bounded by [minPrefixLen] and
     * start-anchoring.
     */
    private fun wordMaxEdits(word: String): Int =
        minOf(
            MAX_TYPO_EDITS,
            when {
                word.length >= 13 -> 3
                word.length >= 9 -> 2
                word.length >= 4 -> 1
                else -> 0
            },
        )

    private enum class Role { PRIMARY, AFFILIATION, RECALL }

    /**
     * Field roles for the exact clause's label: PRIMARY = name-tier fields
     * (mtch_exact), AFFILIATION = bio/website (mtch_affil), RECALL = the
     * rest, unlabeled.
     */
    private fun roleOf(field: String): Role =
        when (field) {
            "name", "display_name", "nip05", "lud16", "search_primary" -> Role.PRIMARY
            "about", "website" -> Role.AFFILIATION
            else -> Role.RECALL
        }

    /**
     * Every column an exact clause searches — the recall surface. NOT private:
     * VespaAppTest reads it back against the shipped schema, so a column added
     * here without a `fieldset default` entry and a `real_match()` term fails
     * the build. A searchable column with no rung is DELETED by
     * text_score_cutoff, not merely ranked low.
     */
    val SEARCH_FIELDS =
        listOf("name", "display_name", "about", "nip05", "lud16", "website", "search_primary", "search_secondary", "search_text", "search_location")

    /**
     * Name-side gram fields, matched ONLY by the tight AND net (the unbounded OR
     * net recalled "model" for "ode"). matchCount feeds the weak tier via
     * infix_gram_match().
     */
    private val INFIX_GRAM_FIELDS = listOf("name_gram", "display_name_gram", "search_primary_gram")

    private val TEXT_GRAM_FIELDS = listOf("about_gram", "search_secondary_gram")

    /**
     * Gram fields matched by PHRASE rather than by the AND net — the body's
     * partial-word reach, and the only reach it has (there is no
     * `search_text_near`; see the field's comment in event.sd for why an
     * attribute is unaffordable on a column filled by every document).
     *
     * NOT private: [SchemaFallbacks] matches 400s against these names, the same
     * way it does for [ALL_NEAR_FIELDS]. Unlike the other gram fields, this one
     * does NOT exist on every deployed schema — it shipped 2026-08-15 — so a
     * reference to it is an HTTP 400 against an older serving schema and needs
     * the same compatibility demotion the near columns get.
     */
    val PHRASE_GRAM_FIELDS = listOf("search_text_gram")
}
