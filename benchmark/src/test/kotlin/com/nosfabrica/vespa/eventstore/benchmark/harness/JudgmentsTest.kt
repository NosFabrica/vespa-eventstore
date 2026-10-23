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
package com.nosfabrica.vespa.eventstore.benchmark.harness

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class JudgmentsTest {
    @Test
    fun `round trips both grade forms and matches by longest prefix`(
        @TempDir dir: Path,
    ) {
        val file = dir.resolve("q.json")
        Files.writeString(
            file,
            """{"_doc": "x", "Avi Burra": {"672c8192": 3, "672c8192aa": {"grade": 1, "by": "llm:test", "at": "2026-10-23"}}}""",
        )
        val j = Judgments.load(file)
        assertEquals(3, j.gradeOf("Avi Burra", "672c8192bb"))
        assertEquals(1, j.gradeOf("Avi Burra", "672c8192aaff"))
        assertNull(j.gradeOf("Avi Burra", "ffff0000"))
        assertNull(j.gradeOf("other", "672c8192"))
        j.write(file)
        val again = Judgments.load(file)
        assertEquals(listOf(3, 1), again.grades("Avi Burra"))
        assertEquals(1, again.topicCount)
    }

    @Test
    fun `an expect becomes a grade-3 judgment only when nothing already judges it`() {
        val j = Judgments.empty()
        j.put("Primal", "6de5328a00", Judgments.Judgment(2))
        val with = j.withImplicit(mapOf("Primal" to ("6de5328a" to 3), "Jack" to ("1f28480d" to 3)))
        assertEquals(2, with.gradeOf("Primal", "6de5328a00ff"))
        assertEquals(3, with.gradeOf("Jack", "1f28480dee"))
        // the original is untouched
        assertEquals(0, j.count("Jack"))
    }

    @Test
    fun `rejects grades off the scale and ids that are not hex`() {
        val j = Judgments.empty()
        assertFailsWith<IllegalArgumentException> { j.put("t", "672c8192", Judgments.Judgment(4)) }
        assertFailsWith<IllegalArgumentException> { j.put("t", "672c", Judgments.Judgment(1)) }
        assertFailsWith<IllegalArgumentException> { j.put("t", "Avi Burra", Judgments.Judgment(1)) }
    }
}
