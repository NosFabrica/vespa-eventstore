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

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * WHETHER A QUERY NAMES AN ENTITY: the test behind the topical-intent probe
 * (docs/embedding-experiment.md §8, `store/search/QueryIntent`). A search is
 * NAVIGATIONAL when someone the reader trusts IS named it, meaning a profile or
 * an app whose declared name is the whole query, and TOPICAL otherwise. Only
 * then does the `search` profile stop paying the perfect-match rung to entities
 * named the topic (event.sd `topical_perfect_drop()`).
 *
 * One definition, used by the store's probe and by `rankAb`'s, so the evaluation
 * measures exactly what ships.
 */
object EntityNames {
    /** The kinds whose declared name an entity is navigated to by: profiles and NIP-89 app handlers. */
    val KINDS: List<Int> = listOf(0, 31990)

    /** The keys a kind 0 or 31990 content object names its entity under. */
    private val NAME_KEYS = listOf("name", "display_name", "displayName")

    private val JSON = Json { ignoreUnknownKeys = true }

    /** The names [content] declares for an event of [kind]: empty for any other kind or unparseable content. */
    fun namesOf(
        kind: Int,
        content: String,
    ): List<String> {
        if (kind !in KINDS) return emptyList()
        val obj = runCatching { JSON.parseToJsonElement(content) as? JsonObject }.getOrNull() ?: return emptyList()
        return NAME_KEYS.mapNotNull { key -> (obj[key] as? JsonPrimitive)?.takeIf { it.isString }?.content?.takeIf { it.isNotBlank() } }
    }

    /**
     * Whether [name] IS [query]: the same words, folded ([NearText.fold]: accents
     * and case) and split on anything that is not a letter or digit, in the same
     * order. "nostr.build" is "Nostr.Build"; "Alby Zapper" is not "alby". The
     * token rule is what the schema's perfect_match() reads too, so the probe
     * and the rung it gates agree on what "named exactly" means.
     */
    fun isExactly(
        query: String,
        name: String,
    ): Boolean {
        val q = tokens(query)
        return q.isNotEmpty() && q == tokens(name)
    }

    /** Whether any name [kind]/[content] declares is exactly [query]. */
    fun names(
        query: String,
        kind: Int,
        content: String,
    ): Boolean = namesOf(kind, content).any { isExactly(query, it) }

    private val SEPARATORS = Regex("[^\\p{L}\\p{N}]+")

    private fun tokens(s: String): List<String> = NearText.fold(s).split(SEPARATORS).filter { it.isNotEmpty() }
}
