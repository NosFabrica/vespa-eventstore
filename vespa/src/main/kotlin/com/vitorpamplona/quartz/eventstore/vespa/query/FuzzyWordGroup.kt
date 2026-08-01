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
 * Per-word recall, extended with the generic tier fields. This is the
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
 *
 * PREFIX / FUZZY are DIRECT terms against the schema's *_parts / *_tokens
 * attribute fields — never annotations on `userInput()`. Ported from
 * Brainstorm's 2026-07-30 root-cause (brainstorm_server#64, verified against a
 * real Vespa): both matchers need TWO things this builder was missing —
 *
 *  1. a DIRECT term. `userInput()` silently drops `prefix`/`fuzzy` annotations
 *     from the terms it builds, so `({defaultIndex:"name",prefix:true}
 *     userInput(@w))` ran as a plain exact match, with no error, ever.
 *  2. an ATTRIBUTE field. Against an `index` field the direct form is rejected
 *     outright ("'name' is not an attribute field: Prefix matching is not
 *     supported"; fuzzy() is HTTP 400).
 *
 * Cause 1 masked cause 2 by turning it into a no-op, which is why "Ode" and
 * "Odel" could never find "ODELL" and nothing ever surfaced. Fields without an
 * attribute sibling (about/website/nip05/lud16, the secondary tiers) stay
 * exact-only: a prefix or fuzzy term against them is an ERROR, not a no-op.
 */
internal object FuzzyWordGroup {
    /**
     * The attribute fields prefix/fuzzy terms match against (see event.sd).
     * name_parts/name_tokens merge name + display_name; the search_primary
     * pair is the generic-tier twin (titles/subjects). Both granularities are
     * load-bearing: *_parts splits at every word start ("meme" finds
     * "BitcoinMemeTreasury"), *_tokens keeps whole tokens ("vitorp" still
     * prefixes "vitorpamplona", which *_parts alone regresses).
     */
    val NEAR_FIELDS = listOf("name_parts", "name_tokens", "search_primary_parts", "search_primary_tokens")

    /**
     * Hard ceiling on typos: a hit needing more edits is not matched at all.
     * Vespa's maxEditDistance enforces it at MATCH time, so an over-budget doc
     * never enters the candidate set — it is not merely ranked low.
     */
    const val MAX_TYPO_EDITS = 3

    /**
     * Minimum word length for a prefix clause. Prefix is a posting-list scan
     * over every term sharing the prefix, so 1-2 Latin characters would sweep
     * a large slice of a big corpus for no precision gain. 3 also matches the
     * trigram floor, so nothing that previously worked stops working.
     */
    const val MIN_PREFIX_LEN = 3

    /**
     * ...but 3 is a LATIN heuristic. A 2-character CJK query ("中村") is as
     * specific as a 5-6 character Latin one, and a flat 3 makes such names
     * unfindable. Any word carrying a non-ASCII character gets the lower
     * floor; those dictionaries are sparse enough that it stays cheap.
     */
    const val MIN_PREFIX_LEN_NON_ASCII = 2

    /**
     * Trigram recall on the NAME-side OR nets — OFF, following upstream.
     *
     * [orGramClause] ORs every trigram of a word, so ANY doc sharing a single
     * 3-character sequence matched: "ode" recalled "model" and "code". It was
     * the one matcher with NO bound on how different a hit could be, and no
     * typo budget can constrain it — it existed to paper over prefix/fuzzy
     * being broken. TRADE-OFF: this drops INFIX matching ("dell" no longer
     * finds "ODELL"); prefix anchors at the start and fuzzy keeps
     * prefixLength:2. Flip back to true to trade the edit-distance bound for
     * that recall — the *_gram fields stay in the schema either way, and the
     * profiles' bm25(*_gram) terms simply read 0 while this is off.
     *
     * The AND nets (about_gram/search_secondary_gram) are unaffected: those
     * fields have no prefix/fuzzy path, and their clause ANDs every trigram —
     * far tighter than this OR — gated by [MIN_AND_GRAMS].
     */
    const val NAME_GRAM_RECALL = false

    /**
     * Minimum trigrams before an AND net is worth emitting (a word of length L
     * yields L-2 trigrams, so this is "word >= 5 chars"). At one or two
     * trigrams the AND degenerates into a bare substring test with no bound at
     * all — upstream measured "ode" reaching a bio reading "hosted by ODELL".
     * Short words still reach those fields through their exact clause.
     */
    const val MIN_AND_GRAMS = 3

    /** All word groups OR'd into one parenthesized clause, filling [params] with the out-of-band words. */
    fun clause(
        words: List<String>,
        params: MutableMap<String, String>,
        nearFields: Boolean = true,
    ): String {
        val groups = ArrayList<String>()
        words.forEachIndexed { i, word ->
            params["w$i"] = word
            groups += wordGroup("@w$i", word, synthetic = false, nearFields = nearFields)
        }
        if (words.size >= 2) {
            val joined = words.joinToString("")
            params["wj"] = joined
            groups += wordGroup("@wj", joined, synthetic = true, nearFields = nearFields)
        }
        if (words.size >= 3) {
            for (i in 0 until words.size - 1) {
                val pair = words[i] + words[i + 1]
                params["wp$i"] = pair
                groups += wordGroup("@wp$i", pair, synthetic = true, nearFields = nearFields)
            }
        }
        return "(${groups.joinToString(" or ")})"
    }

    /** True when the shortest word is short enough to lean harder on the trigram net (drives query(w_gram)). */
    fun leansOnGrams(words: List<String>): Boolean = words.minOf { it.length } <= 3

    /**
     * One word's match clauses: the exact clause per search field, the direct
     * prefix/fuzzy terms against [NEAR_FIELDS], and the AND-gram nets.
     *
     * [synthetic] marks the joined / adjacent-pair CONCATENATIONS built in
     * [clause], not words the user typed. They get exact + prefix but NO fuzzy
     * and no trigrams: a 3-word query builds a 20+ character concatenation
     * that would draw the TOP typo budget — the single most expensive matcher
     * (it walks the attribute dictionary) — for a token nobody typed. Prefix
     * on the concatenation is cheap and does the useful work ("vitor pamplona"
     * → @vitorpamplona), so it stays. The concatenation's trigrams are a
     * superset of the words' own, so the nets would add noise without reach.
     *
     * [nearFields] off drops the prefix/fuzzy clauses entirely — the
     * compatibility demotion for a serving schema that predates the *_parts/
     * *_tokens attributes, where any reference to them is HTTP 400 (see
     * VespaEventIndex.nearSafe).
     */
    private fun wordGroup(
        param: String,
        literal: String,
        synthetic: Boolean,
        nearFields: Boolean,
    ): String {
        val clauses = ArrayList<String>()
        for (field in SEARCH_FIELDS) clauses += exactClause(field, param, roleOf(field))
        if (nearFields) clauses += nearClauses(param, literal, allowFuzzy = !synthetic)
        if (!synthetic) {
            if (NAME_GRAM_RECALL) {
                for (gramField in OR_GRAM_FIELDS) orGramClause(literal, gramField)?.let { clauses += it }
            }
            for (gramField in AND_GRAM_FIELDS) andGramClause(literal, gramField)?.let { clauses += it }
        }
        return "(${clauses.joinToString(" or ")})"
    }

    /**
     * The EXACT clause for one (field, word), against the INDEX field via
     * userInput(). This is what feeds matchCount(name)/matchCount(…), i.e. the
     * schema's exact tier. The labels feed itemRawScore(mtch_*) in the
     * profiles' match-features; prefix/fuzzy are NOT emitted here — they are
     * per-word, not per-field, and go to the merged attribute fields via
     * [nearClauses].
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
     * The direct prefix + fuzzy terms for one word, against [NEAR_FIELDS].
     * Emitted ONCE per word (not per source field) because the attribute
     * fields already merge their sources. These feed matchCount(name_parts)/…,
     * i.e. the schema's near tier — which is how an exact hit still ranks
     * above a prefix or typo hit.
     *
     * prefixLength:2 — the first two characters must match exactly, which also
     * bounds how much of the attribute dictionary the fuzzy matcher walks.
     * Only the TOP edit budget is emitted: maxEditDistance:N subsumes every
     * smaller N, so per-tier clauses would be duplicate matching work (the
     * match_quality labels they once fed are inert — itemRawScore never
     * populates for plain text terms).
     */
    private fun nearClauses(
        param: String,
        literal: String,
        allowFuzzy: Boolean,
    ): List<String> {
        val clauses = ArrayList<String>()
        if (literal.length >= minPrefixLen(literal)) {
            for (field in NEAR_FIELDS) clauses += "($field contains ({prefix:true}$param))"
        }
        val edits = if (allowFuzzy) wordMaxEdits(literal) else 0
        if (edits > 0) {
            for (field in NEAR_FIELDS) clauses += "($field contains ({maxEditDistance:$edits,prefixLength:2}fuzzy($param)))"
        }
        return clauses
    }

    private fun minPrefixLen(word: String): Int = if (word.all { it.code < 128 }) MIN_PREFIX_LEN else MIN_PREFIX_LEN_NON_ASCII

    /** OR of the word's trigrams against a gram field — unbounded infix recall, kept behind [NAME_GRAM_RECALL]. */
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
     * trigram must be present, unlike the OR nets). Used for the long
     * free-text fields — `about` and the generic `search_secondary` — where an
     * OR net would recall too much noise. Gated by [MIN_AND_GRAMS]: fewer
     * trigrams than that and the AND is a bare substring test.
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
        if (grams.size < MIN_AND_GRAMS) return null
        return grams.joinToString(" and ", prefix = "(", postfix = ")") { "$gramField contains \"$it\"" }
    }

    /** Alphanumeric-only trigrams — safe to embed in YQL without escaping. */
    private fun trigrams(word: String): List<String> =
        (0..word.length - 3)
            .map { word.substring(it, it + 3) }
            .filter { gram -> gram.all(Char::isLetterOrDigit) }

    /**
     * Per-word typo budget, length-gated and capped at [MAX_TYPO_EDITS]. The
     * gating is the point: an edit budget is only meaningful as a FRACTION of
     * the word — 3 edits against a 6-letter word matches a different word, not
     * a typo (upstream measured "odelll" → "Odessa"). Every tier holds the
     * ratio near ~22-25%, where Meilisearch sits:
     *
     *     <4     0 edits   any edit on a 3-letter word is a different word
     *     4-8    1         25% .. 12%
     *     9-12   2         22% .. 17%
     *     >=13   3         23% .. less    <- the ceiling, long handles only
     *
     * This budget does NOT govern prefix matching, which is a different
     * relation: "vitorp" → "VitorPamplona" appends 7 characters but is an
     * unfinished word, not 7 typos. Prefix is bounded by [minPrefixLen] and by
     * being anchored at the start.
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
     * Field roles for the exact clause's label. Primary is the name-tier
     * fields: nip05/lud16 are @-address identity fields, and search_primary is
     * the generic-tier twin. Affiliation is bio and website, whose exact
     * clause is labeled mtch_affil. Recall is everything else, which matches
     * without labeling.
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
