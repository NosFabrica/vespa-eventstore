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

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class StemTextTest {
    @Test
    fun `the guess names Portuguese and Spanish bodies, and nothing else`() {
        assertEquals("pt", StemText.guess("Não sei se você viu, mas isso é muito importante para nós"))
        assertEquals("es", StemText.guess("No sé si lo viste, pero esto es muy importante para nosotros"))
        // English, German and French vote too, so their notes are not stemmed as Portuguese or Spanish.
        assertNull(StemText.guess("This is the thing that you have to see"))
        assertNull(StemText.guess("Das ist nicht das, was ich mit dir besprochen habe"))
        // Too little to tell: no stems rather than a guess.
        assertNull(StemText.guess("bitcoin"))
        // Unaccented posts still count: the function words are listed both ways.
        assertEquals("pt", StemText.guess("nao sei se voce viu, mas isso e muito importante"))
    }

    @Test
    fun `plural and singular meet, with the accent or without it`() {
        val doc = StemText.stems("pt", "As eleições e as notícias")!!.split(' ').toSet()
        // The query stems what was typed; the document carries both spellings' stems.
        for (typed in listOf("eleição", "eleições", "eleicoes", "notícia", "noticias")) {
            assertTrue(StemText.stemQuery("pt", typed)!! in doc, "'$typed' must reach the document: $doc")
        }
        assertEquals(StemText.stemQuery("es", "canción"), StemText.stemQuery("es", "canciones"))
        assertEquals(StemText.stemQuery("es", "nueva"), StemText.stemQuery("es", "nuevos"))
    }

    @Test
    fun `short words, numbers and unknown languages get no stem`() {
        assertNull(StemText.stemQuery("pt", "é"))
        assertNull(StemText.stemQuery("pt", "2026"))
        assertNull(StemText.stemQuery("de", "häuser"))
        assertNull(StemText.stems("pt", "a é 12"))
    }

    @Test
    fun `a long body is capped`() {
        val body = (1..5000).joinToString(" ") { "palavra$it".filter(Char::isLetter) + ('a' + it % 26) + ('a' + it / 26 % 26) + ('a' + it / 676 % 26) }
        assertTrue(StemText.stems("pt", body)!!.split(' ').size <= StemText.MAX_STEMS)
    }
}
