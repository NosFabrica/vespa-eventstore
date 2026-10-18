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
package com.nosfabrica.vespa.eventstore.engine.text

import org.apache.lucene.analysis.es.SpanishLightStemmer
import org.apache.lucene.analysis.pt.PortugueseLightStemmer

/**
 * The STEMMED body columns (`search_text_pt`, `search_text_es`): a note's words reduced to
 * their stems, fed by the store for the notes it judges to be in that language, and matched
 * when a search says it is in that language (NIP-50 `language:pt`). A Portuguese searcher
 * typing "notícia" then reaches a note that only says "notícias", which no other column does:
 * the exact column wants the whole token, and the trigram net is a substring test that runs
 * the wrong way for an inflection longer than the query.
 *
 * WHY STEMMED HERE and not by Vespa: Vespa stems with the DOCUMENT's language, and a query
 * carries none, so the two ends disagreed on every non-English note (event.sd, "STEMMING IS
 * OFF"); and the document language is pinned to "en" so that nothing detects it. Doing it
 * here keeps both ends in one place, as [NearText] does for the near tier: the document side
 * ([stems]) and the query side ([stemQuery]) run the SAME stemmer on the same folding.
 *
 * WHY LIGHT STEMMERS: Lucene's own Portuguese and Spanish analyzers use Savoy's light
 * stemmers, which fold plural and gender and little else. MEASURED on the live-relay slice
 * (2026-10-18), against word families from the slice's own notes: Snowball merged far more —
 * `como/comida/comando`, `partir/parte/partido`, `costo/costa`, `sentada/sentir` — which in a
 * search tier is precision traded away; the light stemmers' merges were overwhelmingly
 * `notícia/notícias`, `nuevas/nueva/nuevo`, with a few false ones (`punto/punta`).
 * benchmark/README.md, "Stemmed body columns".
 *
 * BOTH SPELLINGS ARE STEMMED. The Portuguese light stemmer reads accents: "eleições" stems
 * to "eleica" (with "eleição") but an unaccented "eleicoes" to "eleico". Searchers drop
 * accents, so a document carries the stem of each word as written AND of its accent-folded
 * form; the query stems what was typed. Either spelling of the plural then finds it.
 */
object StemText {
    const val PORTUGUESE = "pt"

    const val SPANISH = "es"

    /** The `language:` values this store has a stemmed column for. */
    val LANGUAGES: Set<String> = setOf(PORTUGUESE, SPANISH)

    /** The column a language's stems are fed to and matched in. */
    fun field(language: String): String = "search_text_$language"

    /** Every stemmed column, for the schema fallbacks. */
    val FIELDS: List<String> = LANGUAGES.map(::field)

    /**
     * At most this many distinct stems per document. A note runs to a few hundred words; a
     * long-form article is capped rather than fed whole, since a stem hit only has to exist.
     */
    const val MAX_STEMS = 1024

    /** Shorter words are left alone: the light stemmers do nothing to them anyway. */
    private const val MIN_WORD = 3

    private val WORD = Regex("[\\p{L}\\p{M}]{$MIN_WORD,}")

    private val portuguese = PortugueseLightStemmer()

    private val spanish = SpanishLightStemmer()

    /** [word] stemmed in [language]: lowercased, stemmed, as written (no folding of its own). */
    private fun stem(
        language: String,
        word: String,
    ): String {
        val chars = word.lowercase().toCharArray()
        val len =
            when (language) {
                PORTUGUESE -> portuguese.stem(chars, chars.size)
                SPANISH -> spanish.stem(chars, chars.size)
                else -> chars.size
            }
        return String(chars, 0, len)
    }

    /** The query side: [word] as the column holds it, or null when the word is too short to stem. */
    fun stemQuery(
        language: String,
        word: String,
    ): String? = if (language in LANGUAGES && WORD.matches(word)) stem(language, word) else null

    /**
     * The document side: [text]'s distinct stems in [language], space-joined (each stem one
     * token to Vespa's tokenizer, which then folds it the same way it folds the query term).
     * Null when nothing stems.
     */
    fun stems(
        language: String,
        text: String,
    ): String? {
        val out = LinkedHashSet<String>()
        for (m in WORD.findAll(text)) {
            out += stem(language, m.value)
            if (out.size >= MAX_STEMS) break
            // An ASCII word folds to its own lowercase, which [stem] already took.
            if (m.value.any { it.code >= 0x80 }) out += stem(language, NearText.foldLikeNormalize(m.value))
            if (out.size >= MAX_STEMS) break
        }
        return out.takeIf { it.isNotEmpty() }?.joinToString(" ")
    }

    /**
     * The language [text] is written in, of the ones with a stemmed column, or null. A vote of
     * frequent function words — the cheapest guess that measured usable on short notes, and
     * the reason Vespa's own detector (which cost ~16% of Vespa's CPU per event) is not used.
     * English, German, French and Italian take part only so their notes are not stemmed as
     * Portuguese or Spanish. A winner needs [MIN_VOTES] distinct words and a strict lead.
     *
     * Runs on every body the store feeds, nearly all of which end up unstemmed, so it reads
     * only the first [GUESS_WORDS] words, in place: no lowercased copy of the body, no regex,
     * and a word costs one lookup in [VOTES]. MEASURED on the live-relay slice's 226k kind 1s
     * (2026-10-18): [SearchFields.stemFields] went from 26 to 4 us per note on average.
     */
    fun guess(text: String): String? {
        val votes = IntArray(VOTING.size)
        val counted = HashSet<String>()
        var words = 0
        var i = 0
        while (i < text.length && words < GUESS_WORDS) {
            if (!isWordChar(text[i])) {
                i++
                continue
            }
            val start = i
            while (i < text.length && isWordChar(text[i])) i++
            // A piece of a URL, a domain or a bech32 string is no word: `.com` voted Portuguese
            // ("com", with) on every note carrying a link, and `nevent1qy28…` breaks into letter
            // runs like `y` and `el` between its digits.
            if (start > 0 && (text[start - 1] in URL_JOINERS || text[start - 1].isDigit())) continue
            if (i < text.length && text[i].isDigit()) continue
            if (i + 1 < text.length && text[i] in URL_JOINERS && text[i + 1].isLetterOrDigit()) continue
            words++
            val word = text.substring(start, i).lowercase()
            val languages = VOTES[word] ?: continue
            if (!counted.add(word)) continue
            for (l in VOTING.indices) if (languages and (1 shl l) != 0) votes[l]++
        }
        var best = -1
        var bestVotes = 0
        var second = 0
        for (l in VOTING.indices) {
            if (votes[l] > bestVotes) {
                second = bestVotes
                bestVotes = votes[l]
                best = l
            } else if (votes[l] > second) {
                second = votes[l]
            }
        }
        return VOTING.getOrNull(best)?.takeIf { bestVotes >= MIN_VOTES && bestVotes > second && it in LANGUAGES }
    }

    private const val MIN_VOTES = 2

    /** What joins the parts of a URL, a domain, an address or a `nostr:` link: a word touching one is not prose. */
    private const val URL_JOINERS = "./@:"

    /** A note decides in its first sentences; a long article is not read to the end to vote. */
    private const val GUESS_WORDS = 200

    /** Letters, combining marks and the apostrophe: `[\p{L}\p{M}']`. */
    private fun isWordChar(c: Char): Boolean =
        Character.isLetter(c) ||
            c == '\'' ||
            Character.getType(c).let { it == Character.NON_SPACING_MARK.toInt() || it == Character.COMBINING_SPACING_MARK.toInt() || it == Character.ENCLOSING_MARK.toInt() }

    /**
     * Distinctive function words, each also listed without its accents (searchers and posters drop
     * them). The other languages' lists leave out every word that is also frequent in Portuguese or
     * Spanish (`que`, `mais`, `les`, `es`, `una`): such a word voted against the note it came
     * from, and with the strict lead a tie stems nothing ("Hoje tem mais gente que ontem" read
     * as pt 2, fr 2). French keeps words of its own Portuguese and Spanish lack (`et`, `il`,
     * `sans`, …) so that a French "mais" does not carry its note, and each language has its
     * own frequent words no other voter shares (`em`, `sem`, `pra`; `el`, `y`, `sin`).
     *
     * MEASURED on the slice's 226k kind 1s against the first lists (2026-10-18), together with
     * the URL and digit rules in [guess]: 311 notes newly stemmed, sampled almost all genuine
     * Spanish or Portuguese; 54 no longer, about half of them not Portuguese at all (Japanese,
     * Indonesian, Romanian notes whose links voted `com`) and half short Portuguese headlines
     * that had only their `.com.br` link to vote with; 11 moved between the two, mostly
     * Spanish read as Portuguese. Words Portuguese and Spanish share (`para`, `por`, `todo`, `nos`) are in BOTH
     * lists, where they help either reach [MIN_VOTES] without deciding between them: listed for
     * one alone, they handed a Portuguese note to Spanish.
     */
    private val FUNCTION_WORDS: Map<String, Set<String>> =
        mapOf(
            PORTUGUESE to "não uma para com você está mais isso muito mas tem são também já foi ele ela seu sua pelo pela nos aos isto essa esse ainda porque quando então aqui agora nós eles sobre depois fazer pode coisa por todo nada este em ao sem onde pra seus suas",
            SPANISH to "los las una para con está pero más esto muy por también ya fue él ella sus del al esta este aquí ahora nosotros ellos sobre después hacer puede cosa cuando porque entonces hay pues donde todo nada bien nos el y sin",
            "en" to "the and is to of you that it in for this with my are not have was but just be they what so on at we",
            "de" to "und nicht ist das ich die der ein mit auch sie sind war wir aber noch nur wie auf für eine den dem",
            "fr" to "et il elle leur sans chez été où est pas une pour dans avec vous je nous sont sur qui tout comme plus très aussi être cette ces des du au",
            "it" to "che non per sono questo anche come più della nel gli perché molto ancora tutto essere hanno questa",
        ).mapValues { (_, words) -> words.split(' ').flatMap { listOf(it, NearText.foldLikeNormalize(it)) }.toSet() }

    /** The voting languages, in [FUNCTION_WORDS] order: a language's index is its bit in [VOTES]. */
    private val VOTING: List<String> = FUNCTION_WORDS.keys.toList()

    /** Each function word to the languages it votes for, as a bitmask over [VOTING]. */
    private val VOTES: Map<String, Int> =
        HashMap<String, Int>().apply {
            VOTING.forEachIndexed { l, language -> FUNCTION_WORDS.getValue(language).forEach { merge(it, 1 shl l, Int::or) } }
        }
}
