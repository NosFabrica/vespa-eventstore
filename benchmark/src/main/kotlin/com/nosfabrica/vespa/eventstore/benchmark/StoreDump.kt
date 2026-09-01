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
package com.nosfabrica.vespa.eventstore.benchmark

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * ONE FILTER, THROUGH THE WHOLE STORE, PRINTED — the page a relay would send.
 *
 * [RankAb] talks to the engine directly, which is what makes it a clean A/B of
 * rank profiles, and is also what it cannot show: the search expansion lives in
 * `NostrSemanticsStore`, so a page's spliced rows — the people a Trusted List
 * or a NIP-51 list names — exist only on this path. Every report about "the
 * People tab returned the wrong people" is therefore a claim about a page only
 * this prints.
 *
 * It is the reading half of [ExportLoad]: capture a slice read-only from a
 * relay, replay it here through the store's own write path so `TrustProjection`
 * rebuilds the same tensors, then ask the same filter the client asked.
 *
 *     VESPA_URL=http://localhost:8080 ./gradlew :benchmark:storeDump \
 *       --args="--search 'Verified Human' --kinds 0 --observer 460c25…065c"
 *
 * `--kinds 0` is the People tab; drop it for the Everything tab, where the
 * pointers themselves stay on the page beside the people they name.
 */
object StoreDump {
    @JvmStatic
    fun main(args: Array<String>) =
        runBlocking {
            val opts =
                args
                    .toList()
                    .chunked(2)
                    .filter { it.size == 2 }
                    .associate { it[0] to it[1] }
            val terms = opts["--search"]
            val observer = opts["--observer"]
            val limit = opts["--limit"]?.toIntOrNull() ?: 40
            val kinds = opts["--kinds"]?.split(",")?.mapNotNull { it.trim().toIntOrNull() }.orEmpty()
            if (terms == null) {
                System.err.println("usage: --search <terms> [--kinds 0,30392] [--observer <hex>] [--limit 40]")
                return@runBlocking
            }
            val url = System.getenv("VESPA_URL") ?: "http://localhost:8080"
            val configUrl = System.getenv("VESPA_CONFIG_URL")
            val store = if (configUrl != null) VespaEventStore.open(url = url, configUrl = configUrl) else VespaEventStore.open(url)
            store.use {
                // The search string the relay would build: the terms plus the
                // NIP-50 extensions, so what runs here is what runs there.
                val search = terms + (observer?.let { " observer:$it" } ?: "")
                val page = store.query<Event>(listOf(Filter(kinds = kinds.ifEmpty { null }, search = search, limit = limit)))
                println("filter kinds=${kinds.ifEmpty { "any" }} limit=$limit search=${search.take(80)}")
                println("  %-4s %-34s %-10s %s".format("#", "name / title", "kind", "author"))
                page.forEachIndexed { i, event ->
                    println("  %-4d %-34s %-10d %s".format(i + 1, label(event).take(34), event.kind, event.pubKey.take(8)))
                }
                println("  ${page.size} row(s)")
            }
        }

    /**
     * What to call a row: a `title` tag for the kinds that carry one, the
     * profile name for a kind 0, else the id. Best-effort, like [RankAb]'s —
     * this labels a diagnostic page, it does not parse events.
     */
    private fun label(event: Event): String {
        event.tags.firstOrNull { it.size > 1 && (it[0] == "title" || it[0] == "name") }?.let { return it[1] }
        if (event.kind != 0) return event.id.take(8)
        val profile = runCatching { Json.parseToJsonElement(event.content).jsonObject }.getOrNull()
        return (profile?.get("display_name") ?: profile?.get("name"))?.jsonPrimitive?.content ?: event.id.take(8)
    }
}
