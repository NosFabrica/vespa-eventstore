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
package com.vitorpamplona.quartz.eventstore.vespa.query

/**
 * Per-word fuzzy recall, extended with the generic tier fields. This is the
 * drift-prone half of [EventYql]. It must stay in lockstep with the schema's
 * search fields and match ladder, so it is isolated here from the generic
 * NIP-01/NIP-50 filter-to-YQL assembly. MockVespaEngine's parser guards against
 * drift.
 *
 * There is one OR group per query word: a word that matches ANY field recalls
 * the doc, and ranking sorts the results out. Two extra groups help multi-word
 * queries: a joined-CamelCase variant for 2+ words ("John Carvalho" finds
 * @johncarvalho), and adjacent-pair concatenations for 3+ words.
 *
 * Words go out-of-band as @w0..@wN / @wj / @wp0.. query parameters, never
 * inlined, so no escaping is needed. The trigram literals are filtered to
 * alphanumeric characters, which makes them safe to embed.
 */
internal object FuzzyWordGroup {
    /** All word groups OR'd into one parenthesized clause, filling [params] with the out-of-band words. */
    fun clause(
        words: List<String>,
        params: MutableMap<String, String>,
    ): String {
        val groups = ArrayList<String>()
        words.forEachIndexed { i, word ->
            params["w$i"] = word
            groups += wordGroup("@w$i", word, withGrams = true)
        }
        if (words.size >= 2) {
            val joined = words.joinToString("")
            params["wj"] = joined
            groups += wordGroup("@wj", joined, withGrams = false)
        }
        if (words.size >= 3) {
            for (i in 0 until words.size - 1) {
                val pair = words[i] + words[i + 1]
                params["wp$i"] = pair
                groups += wordGroup("@wp$i", pair, withGrams = false)
            }
        }
        return "(${groups.joinToString(" or ")})"
    }

    /** True when the shortest word is short enough to lean harder on the trigram net (drives query(w_gram)). */
    fun leansOnGrams(words: List<String>): Boolean = words.minOf { it.length } <= 3

    /** One word's match clauses across every search field, plus its trigram safety net. */
    private fun wordGroup(
        param: String,
        literal: String,
        withGrams: Boolean,
    ): String {
        val maxEdits = wordMaxEdits(literal)
        val clauses = ArrayList<String>()
        for (field in SEARCH_FIELDS) clauses += fieldClauses(field, param, maxEdits, roleOf(field))
        if (withGrams) {
            for (gramField in OR_GRAM_FIELDS) orGramClause(literal, gramField)?.let { clauses += it }
            for (gramField in AND_GRAM_FIELDS) andGramClause(literal, gramField)?.let { clauses += it }
        }
        return "(${clauses.joinToString(" or ")})"
    }

    /**
     * Match clauses for one (field, word): exact, prefix, and the length-gated
     * fuzzy tiers. The fuzzy budget is by word length: under 4 chars gets exact
     * or prefix only, 4+ chars one edit, 9+ chars two edits. prefixLength:2
     * means the first two characters must match exactly. Labels feed the
     * schema's match_quality ladder on primary-role fields; they have no effect
     * today but are kept in place.
     */
    private fun fieldClauses(
        field: String,
        param: String,
        maxEdits: Int,
        role: Role,
    ): List<String> {
        fun ann(
            extra: String?,
            label: String?,
        ): String {
            val parts = ArrayList<String>(3)
            parts += "defaultIndex:\"$field\""
            extra?.let { parts += it }
            label?.let { parts += "label:\"$it\"" }
            return parts.joinToString(",", prefix = "{", postfix = "}")
        }

        val exactLabel =
            if (role == Role.PRIMARY) {
                "mtch_exact"
            } else if (role == Role.AFFILIATION) {
                "mtch_affil"
            } else {
                null
            }
        val clauses = ArrayList<String>(4)
        clauses += "(${ann(null, exactLabel)}userInput($param))"
        clauses += "(${ann("prefix:true", if (role == Role.PRIMARY) "mtch_prefix" else null)}userInput($param))"
        if (maxEdits >= 1) {
            clauses += "(${ann("fuzzy:{maxEditDistance:1,prefixLength:2}", if (role == Role.PRIMARY) "mtch_fz1" else null)}userInput($param))"
        }
        if (maxEdits >= 2) {
            clauses += "(${ann("fuzzy:{maxEditDistance:2,prefixLength:2}", if (role == Role.PRIMARY) "mtch_fz2" else null)}userInput($param))"
        }
        return clauses
    }

    /** OR of the word's trigrams against a gram field — the recall safety net. */
    private fun orGramClause(
        word: String,
        gramField: String,
    ): String? {
        val grams = trigrams(word.lowercase()).distinct().sorted()
        if (grams.isEmpty()) return null
        return grams.joinToString(" or ", prefix = "(", postfix = ")") { "$gramField contains \"$it\"" }
    }

    /**
     * AND of the word's trigrams against a discriminative gram field (every
     * trigram must be present, unlike the OR nets). Used for the long free-text
     * fields — `about` and the generic `search_secondary` — where an OR net
     * would recall too much noise.
     */
    private fun andGramClause(
        word: String,
        gramField: String,
    ): String? {
        // Lowercase like every other gram net (orGramClause): the *_gram fields
        // are lowercase-indexed, so uppercased trigrams from a capitalized query
        // word ("Vitor") would never match and this discriminative net would go
        // silently dead for mixed-case input — the common case for names.
        val grams = trigrams(word.lowercase())
        if (grams.isEmpty()) return null
        return grams.joinToString(" and ", prefix = "(", postfix = ")") { "$gramField contains \"$it\"" }
    }

    /** Alphanumeric-only trigrams — safe to embed in YQL without escaping. */
    private fun trigrams(word: String): List<String> =
        (0..word.length - 3)
            .map { word.substring(it, it + 3) }
            .filter { gram -> gram.all(Char::isLetterOrDigit) }

    private fun wordMaxEdits(word: String): Int =
        when {
            word.length >= 9 -> 2
            word.length >= 4 -> 1
            else -> 0
        }

    private enum class Role { PRIMARY, AFFILIATION, RECALL }

    /**
     * Field roles. Primary is the name-tier fields whose clauses carry the
     * match-quality labels: nip05/lud16 are @-address identity fields, and
     * search_primary is the generic-tier twin. Affiliation is bio and website,
     * whose exact clause is labeled mtch_affil. Recall is everything else,
     * which matches without labeling.
     */
    private fun roleOf(field: String): Role =
        when (field) {
            "name", "display_name", "nip05", "lud16", "search_primary" -> Role.PRIMARY
            "about", "website" -> Role.AFFILIATION
            else -> Role.RECALL
        }

    private val SEARCH_FIELDS =
        listOf("name", "display_name", "about", "nip05", "lud16", "website", "search_primary", "search_secondary", "search_text", "search_location")

    private val OR_GRAM_FIELDS = listOf("name_gram", "display_name_gram", "search_primary_gram")

    private val AND_GRAM_FIELDS = listOf("about_gram", "search_secondary_gram")
}
