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
package com.nosfabrica.vespa.eventstore.vespa.query

import com.nosfabrica.vespa.eventstore.vespa.NearText

/**
 * Per-word recall, extended with the generic tier fields. This is the
 * drift-prone half of [EventYql]. It must stay in lockstep with the schema's
 * search fields and match ladder, so it is isolated here from the generic
 * NIP-01/NIP-50 filter-to-YQL assembly. MockVespaEngine's parser guards against
 * drift.
 *
 * There is one match group per query word, and the groups are AND'd: EVERY
 * word must land somewhere on the doc — "Vitor Pamplona" no longer recalls
 * every Vitor and every Pamplona. Within its group a word still matches
 * loosely (any field, through exact/prefix/fuzzy/trigram alike, so a typo'd
 * word still counts as present), and ranking sorts the full-coverage results
 * out. Two extra groups keep concatenated handles reachable for multi-word
 * queries: a joined-CamelCase variant for 2+ words ("John Carvalho" finds
 * @johncarvalho), which satisfies every word at once and is therefore OR'd
 * against the whole conjunction; and adjacent-pair concatenations for 3+
 * words, each standing in for exactly its two words inside the conjunction.
 *
 * Words go out-of-band as query parameters, never inlined, so no escaping is
 * needed: @w0..@wN carry the words AS TYPED (for the exact clauses, whose
 * index fields run Vespa's own linguistic folding) and @f0..@fN carry the
 * [NearText.foldWord]-folded forms (for the near clauses, whose ATTRIBUTE
 * fields match raw bytes — the fold must match the feed's, or "jose" can
 * never reach "josé"). The trigram literals are filtered to alphanumeric
 * characters, which makes them safe to embed.
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
 * "Odel" could never find "ODELL" and nothing ever surfaced. The INDEX fields
 * themselves (about/website/nip05/lud16 included) stay exact-only — a prefix
 * or fuzzy term against them is an ERROR, not a no-op; their prefix reach
 * lives on the affil_tokens attribute sibling ([PREFIX_ONLY_FIELDS]).
 */
internal object FuzzyWordGroup {
    /**
     * The attribute fields prefix AND fuzzy terms match against (see
     * event.sd; fed by NearText). name_parts/name_tokens merge name +
     * display_name; the search_primary pair is the generic-tier twin. Both
     * granularities are load-bearing: *_parts splits at every word start
     * ("meme" finds "BitcoinMemeTreasury"), *_tokens keeps whole tokens plus
     * their joined variants ("vitorp" prefixes "vitorpamplona" for
     * "VitorPamplona", "Vitor-Pamplona" and "Vitor Pamplona" alike).
     */
    val NEAR_FIELDS = listOf("name_parts", "name_tokens", "search_primary_parts", "search_primary_tokens")

    /**
     * Prefix-ONLY attribute fields: hashtag/summary tokens ("bitco" ->
     * #bitcoin) and the identity/affiliation segments of nip05/lud16/website/
     * about ("vitorpamp" -> amethyst@vitorpamplona.com). The affil column is
     * what keeps recall CONTINUOUS while a name is being typed: the docs the
     * finished word reaches through those fields' exact clauses must not
     * vanish for every prefix of it — most of that reach rides the
     * joined-variant prefix ("Vitor Pamp" -> @fwj "vitorpamp" ->
     * "vitorpamplona"), which only sees attribute fields (the 2026-08-02
     * as-you-type ladder report; SearchPrefixLadderIT). No fuzzy on either —
     * secondary/affiliation text builds a much larger dictionary than names
     * do, and a typo'd hashtag or domain is not a query shape worth the
     * dictionary walk. Scored by the schema's WEAK tier, not the near tier.
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
     * Minimum AND'd trigrams for the NAME-side infix nets (word >= 4 chars).
     * AND-of-trigrams is a near-substring test whose selectivity grows with
     * the query — "dell" (del ∧ ell) reaches ODELL without the unbounded OR
     * net's noise ("ode" pulled in "model" and "code"; the OR net is gone).
     * 2 is the floor that keeps "dell"-length infixes reachable; the schema
     * scores these hits in the WEAK tier, under every anchored match.
     */
    const val MIN_AND_GRAMS_NAME = 2

    /**
     * Minimum AND'd trigrams for the long-TEXT nets (about_gram /
     * search_secondary_gram; word >= 5 chars). Long fields contain far more
     * trigrams, so at one or two the AND degenerates into a bare substring
     * test — upstream measured "ode" reaching a bio reading "hosted by
     * ODELL". Short words still reach those fields through their exact
     * clause.
     */
    const val MIN_AND_GRAMS_TEXT = 3

    /**
     * The per-word groups AND'd into one parenthesized clause, filling
     * [params] with the out-of-band words.
     *
     * Each adjacent-pair concatenation covers exactly ITS two words, so it is
     * OR'd into both words' requirements and nowhere else: a "johncarvalho"
     * hit stands in for "john" and "carvalho", but "dev" still has to match
     * on its own. The joined variant covers EVERY word at once, which lets it
     * hoist out of the conjunction — `∧ (reqᵢ ∨ joined)` ≡ `(∧ reqᵢ) ∨
     * joined` — so its group is emitted ONCE instead of once per word
     * (duplicate identical terms would also inflate matchCount, i.e. the
     * exact tier's text score, for every doc the variant matches).
     *
     * The pair groups' two-way ride is the accepted residual of that concern:
     * a pair covers two words, not all, so it cannot hoist without
     * duplicating the real word groups instead (which carry the fuzzy
     * matchers — far worse). The cost is bounded — synthetic groups are
     * exact+prefix only, the shape needs 3+ words, and only docs actually
     * matching the concatenation see the inflated matchCount.
     */
    fun clause(
        words: List<String>,
        params: MutableMap<String, String>,
        nearFields: Boolean = true,
    ): String {
        val own =
            words.mapIndexed { i, word ->
                params["w$i"] = word
                wordGroup("w$i", word, params, synthetic = false, nearFields = nearFields)
            }
        if (words.size == 1) return "(${own[0]})"
        val coverers = List(words.size) { ArrayList<String>() }
        if (words.size >= 3) {
            for (i in 0 until words.size - 1) {
                val pair = words[i] + words[i + 1]
                params["wp$i"] = pair
                val group = wordGroup("wp$i", pair, params, synthetic = true, nearFields = nearFields)
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
        val joinedGroup = wordGroup("wj", joined, params, synthetic = true, nearFields = nearFields)
        return "(($required) or $joinedGroup)"
    }

    /** True when the shortest word is short enough to lean harder on the trigram net (drives query(w_gram)). */
    fun leansOnGrams(words: List<String>): Boolean = words.minOf { it.length } <= 3

    /**
     * One word's match clauses: the exact clause per search field, the direct
     * prefix/fuzzy terms against the near attributes, the prefix-only
     * hashtag/summary clause, and the AND-gram nets.
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
     * [nearFields] off drops every clause that references the near/weak
     * attribute fields — the compatibility demotion for a serving schema that
     * predates them, where any reference is HTTP 400 (see
     * VespaEventIndex.nearSafe). The gram nets are NOT gated: the *_gram
     * fields predate the near tier and exist on every deployed schema.
     */
    private fun wordGroup(
        name: String,
        literal: String,
        params: MutableMap<String, String>,
        synthetic: Boolean,
        nearFields: Boolean,
    ): String {
        val clauses = ArrayList<String>()
        for (field in SEARCH_FIELDS) clauses += exactClause(field, "@$name", roleOf(field))
        if (nearFields) {
            // The folded twin rides out-of-band too, under the f-prefixed name.
            // Floors and budgets are computed on the FOLDED form — that is the
            // string the attribute dictionaries actually hold.
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
     * The direct prefix + fuzzy terms for one FOLDED word, against the near
     * attributes. Emitted ONCE per word (not per source field) because the
     * attribute fields already merge their sources. These feed
     * matchCount(name_parts)/…, i.e. the schema's near tier — which is how an
     * exact hit still ranks above a prefix or typo hit. The prefix-only
     * hashtag/summary field feeds matchCount(search_secondary_tokens), the
     * schema's weak tier.
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
        // Lowercase: the *_gram fields are lowercase-indexed, so uppercased
        // trigrams from a capitalized query word ("Vitor") would never match
        // and the net would go silently dead for mixed-case input.
        val grams = trigrams(word.lowercase())
        if (grams.size < minGrams) return null
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

    /**
     * The name-side gram fields, now matched ONLY by the tight AND net (the
     * unbounded OR net — anything sharing one trigram — is gone for good;
     * "ode" recalled "model" and "code" through it). matchCount on these
     * feeds the schema's weak tier via infix_gram_match().
     */
    private val INFIX_GRAM_FIELDS = listOf("name_gram", "display_name_gram", "search_primary_gram")

    private val TEXT_GRAM_FIELDS = listOf("about_gram", "search_secondary_gram")
}
