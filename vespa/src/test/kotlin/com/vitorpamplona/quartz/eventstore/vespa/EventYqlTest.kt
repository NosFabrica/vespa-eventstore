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
package com.vitorpamplona.quartz.eventstore.vespa
import com.vitorpamplona.quartz.eventstore.vespa.query.EventQuery
import com.vitorpamplona.quartz.eventstore.vespa.query.EventYql
import java.io.ByteArrayInputStream
import java.util.zip.ZipInputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class EventYqlTest {
    private val hexA = "a".repeat(64)
    private val hexB = "b".repeat(64)

    @Test
    fun `no constraints is a match-all ordered by recency`() {
        val q = EventYql.build(EventQuery())!!
        assertEquals("select ${EventYql.SUMMARY_FIELDS} from event where true order by created_at desc", q.yql)
        assertEquals(EventYql.RANK_UNRANKED, q.ranking)
        assertTrue(q.params.isEmpty())
    }

    @Test
    fun `full filter maps every field`() {
        val q =
            EventYql.build(
                EventQuery(
                    kinds = listOf(0, 30382),
                    authors = listOf(hexA),
                    tags = mapOf("p" to listOf(hexB)),
                    since = 100,
                    until = 200,
                    limit = 50,
                ),
            )!!
        assertEquals(
            "select ${EventYql.SUMMARY_FIELDS} from event where kind in (0, 30382) and pubkey in (\"$hexA\") " +
                "and (tag_index contains \"p:$hexB\") and created_at >= 100 and created_at <= 200 " +
                "order by created_at desc limit 50",
            q.yql,
        )
    }

    @Test
    fun `existence queries are summary-free — dedup class, no order, no limit`() {
        val q = EventYql.buildExistence(listOf(hexA.uppercase(), hexB, hexB, "invalid"))!!
        // Order and limit are deliberately absent: membership is unordered, and
        // an existence answer must be complete. Ids normalize (lowercase, dedup)
        // exactly as hexIn does for build().
        assertEquals("select id from event where id in (\"$hexA\", \"$hexB\")", q.yql)
        assertEquals(EventYql.RANK_UNRANKED, q.ranking)
        assertEquals(EventYql.SUMMARY_DEDUP, q.params["presentation.summary"])
        assertNull(EventYql.buildExistence(listOf("not-hex", "")), "no valid id = unsatisfiable, never a wire query")
        assertNull(EventYql.buildExistence(emptyList()))
    }

    @Test
    fun `search words go out-of-band and switch the default ranking on`() {
        val q = EventYql.build(EventQuery(kinds = listOf(0), search = "vitor pamplona"))!!
        assertTrue(q.yql.startsWith("select ${EventYql.SUMMARY_FIELDS} from event where kind in (0) and (((("), q.yql)
        assertEquals("vitor", q.params["w0"])
        assertEquals("pamplona", q.params["w1"])
        assertEquals("vitorpamplona", q.params["wj"], "two words get the joined-CamelCase variant")
        assertFalse("wp0" in q.params, "adjacent pairs only appear from three words up")
        assertEquals("2.0", q.params["ranking.features.query(w_gram)"], "no short word: normal gram weight")
        assertEquals(EventYql.RANK_TEXT, q.ranking, "no observer: search defaults to pure text")
        assertFalse("order by" in q.yql, "ranked queries must not force recency order")
    }

    @Test
    fun `multi-word queries AND the word groups and the joined variant satisfies them all`() {
        // Every word must be present somewhere on the doc — "vitor pamplona"
        // must stop recalling every vitor and every pamplona. Each word's own
        // group keeps its full matcher set (exact/prefix/fuzzy/grams), so a
        // typo'd word still counts as present.
        val two = EventYql.build(EventQuery(search = "vitor pamplona"))!!
        assertTrue(") and (({defaultIndex:\"name\",label:\"mtch_exact\"}userInput(@w1))" in two.yql, two.yql)
        // The joined variant covers both words at once, so it ORs against the
        // whole conjunction — ((w0-group and w1-group) or wj-group) — and its
        // group is emitted exactly once (duplicates would inflate matchCount).
        assertTrue(") or (({defaultIndex:\"name\",label:\"mtch_exact\"}userInput(@wj))" in two.yql, two.yql)
        val wjExact = Regex(Regex.escape("({defaultIndex:\"name\",label:\"mtch_exact\"}userInput(@wj))"))
        assertEquals(1, wjExact.findAll(two.yql).count(), "the joined variant is hoisted, not repeated per word")

        // A pair concatenation stands in for exactly ITS two words: it rides
        // inside both adjacent words' requirements and nowhere else.
        val three = EventYql.build(EventQuery(search = "john carvalho dev"))!!
        val wp0Exact = Regex(Regex.escape("({defaultIndex:\"name\",label:\"mtch_exact\"}userInput(@wp0))"))
        assertEquals(2, wp0Exact.findAll(three.yql).count(), "a pair covers its two words, not the third")
        assertEquals(1, wjExact.findAll(three.yql).count())
    }

    @Test
    fun `words without a letter or digit cannot be required`() {
        // Tokenization erases "⚡" from every matcher's view — userInput emits
        // no term, NearText folds it away, the trigram filter drops it. Under
        // the AND'd word groups an empty requirement would surrender the whole
        // conjunction to Vespa's null-term handling, so the builder drops the
        // word — mirroring what indexing did to the doc side.
        val q = EventYql.build(EventQuery(search = "vitor ⚡"))!!
        assertEquals("vitor", q.params["w0"])
        assertFalse("w1" in q.params, "the symbol word must not become a requirement")
        assertFalse("wj" in q.params, "one matchable word left: no joined variant")
        // A query that is ONLY such words asked for something no index holds.
        assertNull(EventYql.build(EventQuery(search = "⚡ //")))
        // Non-ASCII LETTERS are matchable — the filter is not an ASCII gate.
        assertEquals("中村", EventYql.build(EventQuery(search = "中村 ⚡"))!!.params["w0"])
    }

    @Test
    fun `quoted phrases emit required phrase clauses and rank like search text`() {
        val q = EventYql.build(EventQuery(search = "cat", phrases = listOf("new york", "e-cash")))!!
        // One self-contained REQUIRED phrase term per entry — exact and
        // adjacent, none of the loose words' prefix/fuzzy/gram reach.
        assertTrue("""({defaultIndex:"default",grammar:"phrase"}userInput(@p0))""" in q.yql, q.yql)
        assertTrue("""({defaultIndex:"default",grammar:"phrase"}userInput(@p1))""" in q.yql, q.yql)
        assertEquals("new york", q.params["p0"])
        assertEquals("e-cash", q.params["p1"])
        assertFalse("fuzzy(@p" in q.yql, "a quoted phrase is exact-only")

        // A phrase-only query is a SEARCH: relevance-ranked, never recency recall.
        val alone = EventYql.build(EventQuery(phrases = listOf("new york")))!!
        assertEquals(EventYql.RANK_TEXT, alone.ranking)
        assertFalse("order by" in alone.yql, "ranked queries must not force recency order")
        assertEquals(EventYql.RANK_SEARCH, EventYql.build(EventQuery(phrases = listOf("new york"), observer = hexA))!!.ranking)
    }

    @Test
    fun `an all-erased phrase is an unsatisfiable requirement`() {
        // Same rule as loose words ("⚡" alone), OPPOSITE of notSearch: a
        // REQUIRED phrase no index can hold provably matches nothing —
        // dropping it instead would silently flip the query into match-all.
        assertNull(EventYql.build(EventQuery(phrases = listOf("⚡"))))
        assertNull(EventYql.build(EventQuery(search = "vitor", phrases = listOf("⚡ //"))))
        // A partially-erased phrase rides raw: Vespa's tokenizer drops what
        // indexing dropped, so the emitted phrase equals what docs hold.
        assertEquals("new ⚡ york", EventYql.build(EventQuery(phrases = listOf("new ⚡ york")))!!.params["p0"])
    }

    @Test
    fun `notSearch words emit negated exact clauses with out-of-band params`() {
        val q = EventYql.build(EventQuery(search = "cat", notSearch = listOf("dog", "e-cash")))!!
        // One self-contained negation per word, against the default fieldset —
        // NOT the fuzzy word group: exclusion must never out-reach what the
        // user literally typed, so no prefix/fuzzy/gram matcher may appear on
        // an @n parameter (phrase keeps "e-cash" one adjacent unit).
        assertTrue("""!(({defaultIndex:"default",grammar:"phrase"}userInput(@n0)))""" in q.yql, q.yql)
        assertTrue("""!(({defaultIndex:"default",grammar:"phrase"}userInput(@n1)))""" in q.yql, q.yql)
        assertEquals("dog", q.params["n0"])
        assertEquals("e-cash", q.params["n1"])
        assertFalse("fuzzy(@n" in q.yql, "exclusion is exact-only")
        assertFalse("prefix:true}@n" in q.yql, "exclusion is exact-only")
        assertEquals(EventYql.RANK_TEXT, q.ranking, "the positive term still drives ranking; exclusions are pure filters")
    }

    @Test
    fun `an exclusion-only query subtracts from an explicit match-all and stays unranked`() {
        // YQL's ! is AND-NOT sugar: a negation needs a positive side, so a
        // notSearch-only query (the store's pure "-word" search) gets a
        // spelled-out `true` — and with no search term it is plain recall,
        // newest first, not a ranked search with nothing to score.
        val q = EventYql.build(EventQuery(notSearch = listOf("spam")))!!
        assertTrue(q.yql.contains("where true and !(("), q.yql)
        assertEquals("spam", q.params["n0"])
        assertEquals(EventYql.RANK_UNRANKED, q.ranking)
        assertTrue("order by created_at desc" in q.yql)
        // The same guard covers the other negation-only shape (notKinds).
        assertTrue(EventYql.build(EventQuery(notKinds = listOf(5)))!!.yql.contains("where true and !(kind in (5))"))
    }

    @Test
    fun `a tokenization-erased exclusion is a no-op, not a dead clause`() {
        // The positive-side rule's mirror image with the opposite outcome:
        // "⚡" is in no index, so requiring it is unsatisfiable (null) but
        // EXCLUDING it is vacuous — nothing holds it, nothing is dropped.
        val q = EventYql.build(EventQuery(notSearch = listOf("⚡")))!!
        assertFalse("userInput(@n" in q.yql, q.yql)
        assertTrue(q.yql.contains("where true "), "the erased exclusion leaves plain recall untouched")
    }

    @Test
    fun `an observer switches the search default to the trust profile`() {
        val text = EventYql.build(EventQuery(search = "vitor"))!!
        assertEquals(EventYql.RANK_TEXT, text.ranking, "no observer: pure text")
        assertFalse("query(user_q)" in text.params.keys.joinToString(), "no observer: no trust feature")

        val trust = EventYql.build(EventQuery(search = "vitor", observer = hexA))!!
        assertEquals(EventYql.RANK_SEARCH, trust.ranking, "observer present: blended trust profile")
        assertEquals("{$hexA:1.0}", trust.params["ranking.features.query(user_q)"])
    }

    @Test
    fun `min_rank is emitted only with an observer to gate against`() {
        val noObserver = EventYql.build(EventQuery(search = "vitor", minRank = 2.0))!!
        assertNull(noObserver.params["ranking.features.query(min_rank)"], "no observer: an unguarded floor would drop everything")

        val withObserver = EventYql.build(EventQuery(search = "vitor", observer = hexA, minRank = 2.0))!!
        assertEquals("2.0", withObserver.params["ranking.features.query(min_rank)"])
    }

    @Test
    fun `word groups carry the per-field exact clauses, labels, and gram nets`() {
        val q = EventYql.build(EventQuery(search = "pamplona"))!!
        // Primary-role fields: labeled exact clause against the INDEX field via
        // userInput — this is what feeds matchCount(), i.e. the exact tier.
        assertTrue("({defaultIndex:\"name\",label:\"mtch_exact\"}userInput(@w0))" in q.yql, q.yql)
        assertTrue("({defaultIndex:\"display_name\",label:\"mtch_exact\"}userInput(@w0))" in q.yql)
        assertTrue("({defaultIndex:\"search_primary\",label:\"mtch_exact\"}userInput(@w0))" in q.yql)
        // Affiliation role: exact clause labeled mtch_affil.
        assertTrue("({defaultIndex:\"about\",label:\"mtch_affil\"}userInput(@w0))" in q.yql)
        // Recall role: unlabeled.
        assertTrue("({defaultIndex:\"search_text\"}userInput(@w0))" in q.yql)
        assertTrue("({defaultIndex:\"search_location\"}userInput(@w0))" in q.yql)
        // AND nets against the discriminative about_gram and search_secondary_gram.
        assertTrue("(about_gram contains \"pam\" and about_gram contains \"amp\"" in q.yql)
        assertTrue("(search_secondary_gram contains \"pam\" and search_secondary_gram contains \"amp\"" in q.yql)
    }

    // ------------------------------------------------------------------
    // The near tier: prefix/fuzzy must be DIRECT terms against the *_parts/
    // *_tokens ATTRIBUTE fields. The old annotated-userInput forms parse, run,
    // and silently behave as plain exact matches (Brainstorm's 2026-07-30
    // root-cause, verified against a real Vespa) — asserting the SHAPE is the
    // point; a test that only checked "does a prefix clause exist" would have
    // passed throughout the outage.
    // ------------------------------------------------------------------

    @Test
    fun `prefix is a direct term on the near fields, never a userInput annotation`() {
        val q = EventYql.build(EventQuery(search = "odell"))!!
        assertTrue("(name_parts contains ({prefix:true}@fw0))" in q.yql, q.yql)
        assertTrue("(name_tokens contains ({prefix:true}@fw0))" in q.yql)
        assertTrue("(search_primary_parts contains ({prefix:true}@fw0))" in q.yql)
        assertTrue("(search_primary_tokens contains ({prefix:true}@fw0))" in q.yql)
        // Hashtag/summary tokens get prefix reach too ("bitco" -> #bitcoin)…
        assertTrue("(search_secondary_tokens contains ({prefix:true}@fw0))" in q.yql)
        // The form that parses, runs, and does nothing.
        assertFalse("prefix:true}userInput" in q.yql)
    }

    @Test
    fun `fuzzy is a direct term on the near fields, never a userInput annotation`() {
        val q = EventYql.build(EventQuery(search = "odelling"))!!
        assertTrue("(name_parts contains ({maxEditDistance:1,prefixLength:2}fuzzy(@fw0)))" in q.yql, q.yql)
        assertTrue("(name_tokens contains ({maxEditDistance:1,prefixLength:2}fuzzy(@fw0)))" in q.yql)
        assertFalse("fuzzy:{maxEditDistance" in q.yql)
        // …but never fuzzy: a typo'd hashtag is not worth walking that dictionary.
        assertFalse("search_secondary_tokens contains ({maxEditDistance" in q.yql)
    }

    @Test
    fun `near clauses carry the FOLDED word out-of-band, exact clauses the original`() {
        // The near attributes match raw bytes (no linguistic folding), so the
        // near params must be diacritic-folded to reach "josé"; the exact
        // clauses keep the typed form for the index fields' own linguistics.
        val q = EventYql.build(EventQuery(search = "José"))!!
        assertEquals("José", q.params["w0"])
        assertEquals("jose", q.params["fw0"])
        assertTrue("({prefix:true}@fw0)" in q.yql)
    }

    @Test
    fun `near terms never target the index-only fields`() {
        // about/website/nip05/lud16 and the secondary tiers have no attribute
        // sibling: a prefix or fuzzy term against them is an ERROR (HTTP 400),
        // not a no-op.
        val q = EventYql.build(EventQuery(search = "something"))!!
        for (field in listOf("name", "display_name", "about", "nip05", "lud16", "website", "search_primary", "search_secondary", "search_text")) {
            assertFalse("$field contains ({prefix:true}" in q.yql, field)
            assertFalse("$field contains ({maxEditDistance" in q.yql, field)
        }
    }

    @Test
    fun `typo budget is length-gated and capped, and only the top distance is emitted`() {
        // <4: no fuzzy at all.
        assertFalse("fuzzy(" in EventYql.build(EventQuery(search = "ode"))!!.yql)
        // 4-8: one edit.
        assertTrue("maxEditDistance:1" in EventYql.build(EventQuery(search = "odel"))!!.yql)
        // 9-12: two edits — and maxEditDistance:2 subsumes 1, so no duplicate tier.
        val two = EventYql.build(EventQuery(search = "odellington"))!!
        assertTrue("maxEditDistance:2" in two.yql)
        assertFalse("maxEditDistance:1" in two.yql, "per-tier clauses are duplicate matching work")
        // >=13: the ceiling (MAX_TYPO_EDITS = 3), even for absurd lengths.
        assertTrue("maxEditDistance:3" in EventYql.build(EventQuery(search = "decentralization"))!!.yql)
        val absurd = EventYql.build(EventQuery(search = "a".repeat(60)))!!
        assertTrue("maxEditDistance:3" in absurd.yql)
        assertFalse("maxEditDistance:4" in absurd.yql)
    }

    @Test
    fun `prefix floor is 3 for latin and 2 for non-ascii`() {
        assertFalse("prefix:true" in EventYql.build(EventQuery(search = "od"))!!.yql)
        assertTrue("prefix:true" in EventYql.build(EventQuery(search = "ode"))!!.yql)
        // A 2-character CJK query is as specific as a 5-6 character Latin one;
        // the Latin floor made such names unreachable.
        assertTrue("prefix:true" in EventYql.build(EventQuery(search = "中村"))!!.yql)
    }

    @Test
    fun `name-side grams are a bounded AND infix net, never the unbounded OR`() {
        // The OR net (anything sharing a single trigram: "ode" -> "model") is
        // gone. In its place, an AND of every trigram — a near-substring test
        // — restores "dell" -> ODELL with a bound, from 2 trigrams (4 chars).
        val short = EventYql.build(EventQuery(search = "ode"))!!
        assertFalse("name_gram contains" in short.yql, "1 trigram is a bare substring probe")
        val dell = EventYql.build(EventQuery(search = "dell"))!!
        assertTrue("(name_gram contains \"del\" and name_gram contains \"ell\")" in dell.yql, dell.yql)
        assertTrue("(display_name_gram contains \"del\" and display_name_gram contains \"ell\")" in dell.yql)
        assertTrue("(search_primary_gram contains \"del\" and search_primary_gram contains \"ell\")" in dell.yql)
        assertFalse("name_gram contains \"del\" or" in dell.yql, "no OR form anywhere")
        // The long-text nets keep the higher floor: at 1-2 trigrams they
        // degenerate into substring tests — "ode" reached a bio reading
        // "hosted by ODELL". >=3 only (word >= 5).
        assertFalse("about_gram contains" in dell.yql)
        assertFalse("about_gram contains" in EventYql.build(EventQuery(search = "odel"))!!.yql)
        assertTrue("about_gram contains" in EventYql.build(EventQuery(search = "odell"))!!.yql)
    }

    @Test
    fun `synthetic concatenations get prefix but not fuzzy`() {
        // The joined/pair variants are built from words nobody typed, and they
        // are long — they would draw the TOP typo budget, the single most
        // expensive matcher, six times over on a 3-word query. Prefix on them
        // is cheap and does the useful work ("vitor pamplona" -> @vitorpamplona).
        val q = EventYql.build(EventQuery(search = "satoshi nakamoto bitcoin"))!!
        assertTrue("prefix:true}@fwj" in q.yql)
        assertTrue("prefix:true}@fwp0" in q.yql)
        for (v in listOf("@fwj", "@fwp0", "@fwp1")) {
            assertFalse("fuzzy($v)" in q.yql, v)
        }
        // One fuzzy clause per real word per near field, nothing more.
        val fields = 4 // name_parts, name_tokens, search_primary_parts, search_primary_tokens
        assertEquals(3 * fields, Regex("fuzzy\\(").findAll(q.yql).count())
    }

    @Test
    fun `nearMatching off drops every near clause — the pre-schema demotion`() {
        // Against a serving schema that predates the *_parts/*_tokens fields,
        // any YQL naming them is HTTP 400 on every search. The demoted query
        // must carry no reference to them at all.
        val q = EventYql.build(EventQuery(search = "odell pamplona", nearMatching = false))!!
        assertFalse("name_parts" in q.yql)
        assertFalse("name_tokens" in q.yql)
        assertFalse("search_primary_parts" in q.yql)
        assertFalse("search_secondary_tokens" in q.yql)
        assertFalse("prefix:true" in q.yql)
        assertFalse("fuzzy(" in q.yql)
        // The exact clauses and gram nets stay — those fields predate the near
        // tier on every deployed schema. Recall degrades, it doesn't die.
        assertTrue("({defaultIndex:\"name\",label:\"mtch_exact\"}userInput(@w0))" in q.yql)
        assertTrue("name_gram contains" in q.yql)
        assertTrue("about_gram contains" in q.yql)
    }

    @Test
    fun `fuzzy budget is length-gated`() {
        val short = EventYql.build(EventQuery(search = "bob"))!!
        assertFalse("fuzzy" in short.yql, "under 4 chars: exact and prefix only")
        assertEquals("8.0", short.params["ranking.features.query(w_gram)"], "short word leans on the gram net")
        assertFalse("wj" in short.params, "a single word has no joined variant")

        val long = EventYql.build(EventQuery(search = "vertexlab"))!!
        assertTrue("maxEditDistance:2,prefixLength:2}fuzzy(" in long.yql, "9+ chars: two edits")
    }

    @Test
    fun `three or more words add adjacent-pair variants and no word is dropped`() {
        val q = EventYql.build(EventQuery(search = "john carvalho dev one two three seven eight"))!!
        assertEquals("johncarvalho", q.params["wp0"])
        assertEquals("carvalhodev", q.params["wp1"])
        // Every word the caller typed reaches the query — the builder imposes no
        // word cap, so the joined variant spans the whole term.
        assertEquals("eight", q.params["w7"])
        assertFalse("w8" in q.params, "exactly as many word params as words")
        assertEquals("johncarvalhodevonetwothreeseveneight", q.params["wj"], "the joined variant covers every word")
        assertEquals("seveneight", q.params["wp6"], "adjacent pairs run to the last word")
    }

    private fun groupingQueries() =
        listOf(
            EventYql.buildDistinctAuthors(EventQuery())!!,
            EventYql.buildKindHistogram(EventQuery())!!,
            EventYql.buildCount(EventQuery())!!,
            EventYql.buildDistinctCount(EventQuery(), "pubkey")!!,
        )

    /** Aggregations must answer over the whole match set: Vespa defaults to TEN groups without these. */
    @Test
    fun `grouping queries disable the per-request group ceilings`() {
        for (q in groupingQueries()) {
            assertEquals(EventYql.UNLIMITED_GROUPS, q.params["grouping.defaultMaxGroups"])
            assertEquals(EventYql.UNLIMITED_GROUPS, q.params["grouping.defaultMaxHits"])
        }
        assertFalse("max(" in EventYql.buildDistinctAuthors(EventQuery())!!.yql, "no group cap in the pipeline")
        assertFalse("max(" in EventYql.buildKindHistogram(EventQuery())!!.yql, "no group cap in the pipeline")
    }

    /**
     * Regression: Vespa's `GroupingQueryParser.validate` rejects ANY request that
     * carries `grouping.globalMaxGroups` — "must be specified in a query profile"
     * — with a 400. Sending it made every aggregation fail against a real Vespa
     * (the parity IT's `count` checks) while the mock engine, which ignores query
     * parameters, stayed green. It belongs in the bundled query profile only.
     */
    @Test
    fun `grouping queries never send the global ceiling as a query parameter`() {
        for (q in groupingQueries()) {
            assertFalse(EventYql.GLOBAL_MAX_GROUPS in q.params, "${EventYql.GLOBAL_MAX_GROUPS} is a 400 when sent with the request")
        }
    }

    /**
     * …and the query profile — the only place Vespa accepts it — must actually
     * disable it. Read out of the bundled zip, i.e. the bytes that get deployed,
     * so an app package that drops the profile fails here and not in the parity IT.
     */
    @Test
    fun `the deployed query profile disables the global group ceiling`() {
        val profile =
            ZipInputStream(ByteArrayInputStream(VespaApp.zipBytes())).use { zip ->
                generateSequence { zip.nextEntry }
                    .firstOrNull { it.name == "search/query-profiles/default.xml" }
                    ?.let { zip.readBytes().decodeToString() }
            }
        assertNotNull(profile, "the application package ships no default query profile")
        assertTrue(
            """<field name="${EventYql.GLOBAL_MAX_GROUPS}">${EventYql.UNLIMITED_GROUPS}</field>""" in profile,
            "the max()-less aggregation pipelines are rejected outright while this ceiling is on",
        )
    }

    @Test
    fun `ranking override without a term is a trust-ordered match-all`() {
        val q = EventYql.build(EventQuery(ranking = EventYql.RANK_DESC, minRank = 2.0, observer = hexA))!!
        assertEquals("select ${EventYql.SUMMARY_FIELDS} from event where true", q.yql)
        assertEquals(EventYql.RANK_DESC, q.ranking)
        assertEquals("{$hexA:1.0}", q.params["ranking.features.query(user_q)"])
        assertEquals("2.0", q.params["ranking.features.query(min_rank)"])
        assertFalse("order by" in q.yql, "a rank profile owns the order")
    }

    @Test
    fun `the observer gate profile carries the lens and floor, and owns the order`() {
        val q = EventYql.build(EventQuery(kinds = listOf(1), limit = 50, ranking = EventYql.RANK_RECENCY_GATED, minRank = 2.0, observer = hexA))!!
        assertEquals(EventYql.RANK_RECENCY_GATED, q.ranking, "small recent limit: the match-phase variant")
        assertEquals("{$hexA:1.0}", q.params["ranking.features.query(user_q)"])
        assertEquals("2.0", q.params["ranking.features.query(min_rank)"])
        assertFalse("order by" in q.yql, "the profile's created_at score is the order")
    }

    @Test
    fun `gated recall demotes to the exact profile when the match-phase cut is unsound`() {
        fun gated(
            limit: Int? = null,
            until: Long? = null,
        ) = EventYql.build(EventQuery(kinds = listOf(1), limit = limit, until = until, ranking = EventYql.RANK_RECENCY_GATED, minRank = 2.0, observer = hexA))!!

        assertEquals(EventYql.RANK_RECENCY_GATED, gated(limit = 50).ranking, "the hot feed shape rides the cut")
        assertEquals(EventYql.RANK_RECENCY_GATED_EXACT, gated(limit = null).ranking, "unlimited: the cut would lose old hits")
        assertEquals(EventYql.RANK_RECENCY_GATED_EXACT, gated(limit = 50_000).ranking, "limit past the headroom")
        val ancient = System.currentTimeMillis() / 1000 - 90 * 86_400L
        assertEquals(EventYql.RANK_RECENCY_GATED_EXACT, gated(limit = 50, until = ancient).ranking, "deep pagination anchors below the cut")
        // Both variants still carry the lens and the floor.
        val exact = gated(limit = null)
        assertEquals("{$hexA:1.0}", exact.params["ranking.features.query(user_q)"])
        assertEquals("2.0", exact.params["ranking.features.query(min_rank)"])
    }

    @Test
    fun `observer is ranking context only — never emitted for unranked recall`() {
        val unranked = EventYql.build(EventQuery(kinds = listOf(1), observer = hexA))!!
        assertTrue(unranked.params.isEmpty(), "no term, no profile: pure NIP-01 recall")
        assertEquals(EventYql.RANK_UNRANKED, unranked.ranking)

        val ranked = EventYql.build(EventQuery(search = "vitor", observer = hexA))!!
        assertEquals("{$hexA:1.0}", ranked.params["ranking.features.query(user_q)"])
    }

    @Test
    fun `rerankCount rides out-of-band as a ranking parameter`() {
        val q = EventYql.build(EventQuery(search = "vitor", ranking = "text2", rerankCount = 500))!!
        assertEquals("500", q.params["ranking.rerankCount"])
        assertEquals("text2", q.ranking)
        assertNull(EventYql.build(EventQuery(kinds = listOf(1)))!!.params["ranking.rerankCount"], "absent unless set")
    }

    @Test
    fun `owners and expiry map to their attributes`() {
        val q = EventYql.build(EventQuery(owners = listOf(hexA), expiresBefore = 500))!!
        assertEquals("select ${EventYql.SUMMARY_FIELDS} from event where owner in (\"$hexA\") and expires_at < 500 order by created_at desc", q.yql)
        assertNull(EventYql.build(EventQuery(owners = listOf("not-hex"))), "no valid owner")
    }

    @Test
    fun `tagsAll requires every value`() {
        val q = EventYql.build(EventQuery(tagsAll = mapOf("t" to listOf("a", "b"))))!!
        assertEquals(
            "select ${EventYql.SUMMARY_FIELDS} from event where (tag_index contains \"t:a\" and tag_index contains \"t:b\") order by created_at desc",
            q.yql,
        )
    }

    @Test
    fun `tag values are OR within a name and AND across names`() {
        // Multi-value OR compiles to the `in` operator (one dictionary-backed
        // iterator over the fast-search attribute); a single value stays `contains`.
        val q = EventYql.build(EventQuery(tags = mapOf("p" to listOf(hexA, hexB), "t" to listOf("nostr"))))!!
        assertEquals(
            "select ${EventYql.SUMMARY_FIELDS} from event where tag_index in (\"p:$hexA\", \"p:$hexB\") " +
                "and (tag_index contains \"t:nostr\") order by created_at desc",
            q.yql,
        )
    }

    @Test
    fun `a wide tag list compiles to one in-list, values escaped`() {
        val q = EventYql.build(EventQuery(tags = mapOf("e" to listOf("v1", "v\"2", "v3"))))!!
        assertEquals(
            "select ${EventYql.SUMMARY_FIELDS} from event where tag_index in (\"e:v1\", \"e:v\\\"2\", \"e:v3\") order by created_at desc",
            q.yql,
        )
    }

    @Test
    fun `invalid hex entries are dropped but valid ones survive`() {
        val q = EventYql.build(EventQuery(ids = listOf("nope", hexA, hexA.uppercase())))!!
        assertEquals("select ${EventYql.SUMMARY_FIELDS} from event where id in (\"$hexA\") order by created_at desc", q.yql)
    }

    @Test
    fun `unsatisfiable constraints build nothing`() {
        assertNull(EventYql.build(EventQuery(authors = listOf("not-hex"))), "no valid author")
        assertNull(EventYql.build(EventQuery(ids = listOf("55"))), "short id")
        assertNull(EventYql.build(EventQuery(tags = mapOf("pp" to listOf("x")))), "multi-letter tag name")
        assertNull(EventYql.build(EventQuery(tags = mapOf("§" to listOf("x")))), "non-ascii tag name")
        assertNull(EventYql.build(EventQuery(tags = mapOf("p" to emptyList()))), "present-but-empty tag values")
        assertNull(EventYql.build(EventQuery(limit = 0)), "limit 0")
    }

    @Test
    fun `caller-supplied strings cannot break out of their literal`() {
        val q = EventYql.build(EventQuery(tags = mapOf("t" to listOf("""x" or true or tag_index contains "y"""))))!!
        assertEquals(
            "select ${EventYql.SUMMARY_FIELDS} from event where (tag_index contains \"t:x\\\" or true or tag_index contains \\\"y\") " +
                "order by created_at desc",
            q.yql,
        )
        val newline = EventYql.build(EventQuery(tags = mapOf("t" to listOf("a\nb\\c"))))!!
        assertTrue("tag_index contains \"t:a\\nb\\\\c\"" in newline.yql)
    }
}
