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
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class EntityNamesTest {
    @Test
    fun `a name is the query when its words are, folded and in order`() {
        assertTrue(EntityNames.isExactly("nostr.build", "Nostr.Build"))
        assertTrue(EntityNames.isExactly("jose", "José"))
        assertTrue(EntityNames.isExactly("Avi Burra", "avi  burra"))
        assertTrue(EntityNames.isExactly("中村", "中村"))
    }

    @Test
    fun `a name that holds more, fewer or other words is not the query`() {
        assertFalse(EntityNames.isExactly("alby", "Alby Zapper"))
        assertFalse(EntityNames.isExactly("bitcoin white paper", "bitcoin"))
        assertFalse(EntityNames.isExactly("burra avi", "Avi Burra"))
        assertFalse(EntityNames.isExactly("", ""), "no words names nothing")
        assertFalse(EntityNames.isExactly("₿", "₿"), "a query of separators names nothing")
    }

    @Test
    fun `profiles and app handlers declare names, every other kind declares none`() {
        assertEquals(listOf("odell", "ODELL"), EntityNames.namesOf(0, """{"name":"odell","display_name":"ODELL","about":"x"}"""))
        assertEquals(listOf("Amethyst"), EntityNames.namesOf(31990, """{"name":"Amethyst","picture":""}"""))
        assertEquals(emptyList(), EntityNames.namesOf(1, """{"name":"odell"}"""))
        assertEquals(emptyList(), EntityNames.namesOf(0, "not json"))
        assertEquals(emptyList(), EntityNames.namesOf(0, """{"name":7,"display_name":""}"""))
    }

    @Test
    fun `names reads the content and compares in one step`() {
        assertTrue(EntityNames.names("odell", 0, """{"display_name":"ODELL"}"""))
        assertFalse(EntityNames.names("coffee", 0, """{"name":"Satoshi Coffee Co."}"""))
    }
}
