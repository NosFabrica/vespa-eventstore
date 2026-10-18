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
            out += stem(language, NearText.foldLikeNormalize(m.value))
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
     */
    fun guess(text: String): String? {
        val words = WORDS.findAll(text.lowercase()).map { it.value }.toHashSet()
        if (words.size < MIN_VOTES) return null
        var best: String? = null
        var bestVotes = 0
        var second = 0
        for ((language, list) in FUNCTION_WORDS) {
            val votes = list.count { it in words }
            if (votes > bestVotes) {
                second = bestVotes
                bestVotes = votes
                best = language
            } else if (votes > second) {
                second = votes
            }
        }
        return best?.takeIf { bestVotes >= MIN_VOTES && bestVotes > second && it in LANGUAGES }
    }

    private const val MIN_VOTES = 2

    private val WORDS = Regex("[\\p{L}\\p{M}']+")

    /** Distinctive function words, each also listed without its accents (searchers and posters drop them). */
    private val FUNCTION_WORDS: Map<String, Set<String>> =
        mapOf(
            PORTUGUESE to "não uma para com você está mais isso muito mas tem são também já foi ele ela seu sua pelo pela nos aos isto essa esse ainda porque quando então aqui agora nós eles sobre depois fazer pode coisa",
            SPANISH to "los las una para con está pero más esto muy por también ya fue él ella sus del al esta este aquí ahora nosotros ellos sobre después hacer puede cosa cuando porque entonces hay pues donde todo nada bien",
            "en" to "the and is to of you that it in for this with my are not have was but just be they what so on at we",
            "de" to "und nicht ist das ich die der ein mit auch sie es sind war wir aber noch nur wie auf für eine den dem",
            "fr" to "les est pas une pour que dans avec mais vous je nous sont sur qui tout comme plus très aussi être cette ces des du au",
            "it" to "che non per una sono questo anche come più della nel gli perché molto ancora tutto essere hanno questa",
        ).mapValues { (_, words) -> words.split(' ').flatMap { listOf(it, NearText.foldLikeNormalize(it)) }.toSet() }
}
