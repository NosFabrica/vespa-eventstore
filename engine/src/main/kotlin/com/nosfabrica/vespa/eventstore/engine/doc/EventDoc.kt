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
package com.nosfabrica.vespa.eventstore.engine.doc
import com.vitorpamplona.quartz.nip01Core.core.Address
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import com.vitorpamplona.quartz.nip01Core.tags.isIndexableTagName
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put

/**
 * One live Nostr event as a Vespa `event` document (docid = [id]). NIP-01
 * fields are held LOSSLESSLY — the signature is over these exact values, so
 * [toEventJson] rebuilds a complete event clients re-verify; no raw-blob copy.
 * [tags] is the exact tag array; the queryable `tag_index` ([tagIndex]) is a
 * derived, lossy view (single-letter names, first values) for `#x` recall,
 * never reconstruction. Signatures are verified BEFORE construction — the
 * index holds only verified events. [owner] and [search] are store-derived:
 * owner is the pubkey semantics key off (gift-wrap recipient for kind 1059,
 * else the author); [search] all-null means invisible to NIP-50 search.
 */
data class EventDoc(
    val id: HexKey,
    val pubkey: HexKey,
    val createdAt: Long,
    val kind: Int,
    val tags: List<List<String>>,
    val content: String,
    val sig: String,
    val owner: HexKey = pubkey,
    val search: SearchFields = SearchFields.NONE,
) {
    /**
     * DECODE METADATA, not document state: the near-tier attribute arrays as
     * actually STORED on the engine doc this was decoded from. null = no
     * evidence (constructed docs; search summaries, which don't carry them).
     * An empty map from a document-API read means genuinely none stored — the
     * pre-near-tier corpus the full-text reindex must re-feed (stale when
     * != [SearchFields.nearFieldsWritten]). Deliberately NOT a constructor
     * property: it records what one read saw, and two reads of the same
     * logical doc must stay equal — so it lives outside equals/copy.
     */
    var storedNearFields: Map<String, List<String>>? = null

    /** [tags] as Quartz's `String[][]` shape, for its tag-array helpers and event reconstruction. */
    private fun tagsArray(): Array<Array<String>> = Array(tags.size) { tags[it].toTypedArray() }

    /**
     * The queryable `"<letter>:<value>"` pairs: one per single-ASCII-letter tag
     * name (what NIP-01 `#x` filters can address) with a value. Everything else
     * still round-trips through [tags].
     */
    fun tagIndex(): List<String> = ArrayList<String>(tags.size).apply { forEachIndexedTag { name, value -> add("$name:$value") } }

    /** Each [tagIndex] entry as its name and value, unjoined: the one place the rule lives. */
    private inline fun forEachIndexedTag(block: (name: String, value: String) -> Unit) {
        for (tag in tags) {
            if (tag.size > 1 && isIndexableTagName(tag[0])) block(tag[0], tag[1])
        }
    }

    /**
     * The NIP-40 expiration; null = never. Direct scan matching Quartz's
     * `expiration()` without [tagsArray]'s deep copy — called on every put.
     */
    fun expiresAt(): Long? =
        tags.firstNotNullOfOrNull { tag ->
            if (tag.size > 1 && tag[0] == "expiration" && tag[1].isNotEmpty()) tag[1].toLongOrNull() else null
        }

    /**
     * The `d` tag value, or "" when absent — the addressable bucketing key
     * (missing == empty). Direct scan matching Quartz's `dTag()` with no
     * `String[][]` copy; on the address/supersession hot path.
     */
    fun dTagOrEmpty(): String = tags.firstOrNull { it.size > 1 && it[0] == "d" }?.get(1) ?: ""

    /** The first `d` tag's value — an addressable event's identity key; null/blank = none. */
    fun dTag(): String? = dTagOrEmpty().takeIf { it.isNotEmpty() }

    /**
     * The NIP-01 address (`kind:pubkey[:dtag]`) replaceable/addressable kinds
     * supersede on — and their docid under address-keyed storage; null for
     * regular events. Replaceables use the fixed empty d-tag.
     */
    fun addressOrNull(): String? =
        when {
            kind.isReplaceable() -> Address.assemble(kind, pubkey)
            kind.isAddressable() -> Address.assemble(kind, pubkey, dTagOrEmpty())
            else -> null
        }

    /**
     * The document's field map — one shape for both feeding and summary parsing
     * ([fromSummary]).
     *
     * [includeNear] off omits the near-tier arrays, for feeding a schema that
     * predates them: Vespa rejects the WHOLE document with a 400 naming the
     * first unknown field, so one missing column would otherwise fail every
     * insert of a searchable event (VespaEventIndex's write-side net, the twin
     * of SchemaFallbacks' read-side demotion). reindexFullTextSearch repairs
     * docs written that way — an omitted column reads back as drift.
     *
     * [includeEdge] (default OFF) adds the PROTOTYPE `search_text_edge` column.
     * Unlike the near tier it can never read back as drift (it is index-only), so
     * a document written without it is repaired only by
     * `NostrSemanticsStore.backfillBodyEdge`.
     *
     * [includeStem] adds the stemmed body column of a Portuguese or Spanish body
     * ([SearchFields.stemFields]), on its own gate for the same reason: its own
     * schema generation, which a schema predating it refuses whole.
     */
    fun indexFields(
        includeNear: Boolean = true,
        includeEdge: Boolean = false,
        includeStem: Boolean = true,
    ): JsonObject =
        buildJsonObject {
            put("id", id)
            put("pubkey", pubkey)
            put("created_at", createdAt)
            put("kind", kind)
            put("tags", tagsJson())
            put("tag_index", JsonArray(tagIndex().map(::JsonPrimitive)))
            put("content", content)
            put("sig", sig)
            put("owner", owner)
            // The author's ranking state (global reputation parent) — purely
            // pubkey-derived, so stamped here rather than by an extractor.
            put("author_ref", "id:reputation:reputation::$pubkey")
            for ((field, value) in search.fields()) put(field, value)
            // Near-tier attribute arrays (prefix/fuzzy targets), derived HERE
            // rather than schema-side so doc and query share NearText's one
            // normalization (see NearText). Existing corpora need a RE-FEED —
            // NostrSemanticsStore.reindexFullTextSearch compares
            // [storedNearFields] against this derivation.
            if (includeNear) {
                for ((field, elements) in search.nearFieldsWritten()) {
                    put(field, JsonArray(elements.map(::JsonPrimitive)))
                }
            }
            // PROTOTYPE (#161): the body's edge n-grams, fed like the near arrays
            // because Vespa's indexing language cannot derive them — but behind
            // their OWN gate: a different schema generation, opt-in, and refused
            // independently of the near tier (VespaEventIndex.bodyEdgeFeed).
            if (includeEdge) {
                for ((field, elements) in search.edgeFields()) {
                    put(field, JsonArray(elements.map(::JsonPrimitive)))
                }
            }
            if (includeStem) {
                for ((field, value) in search.stemFields()) put(field, value)
            }
            // Always written. An absent numeric attribute reads as 0 in Vespa,
            // which would make "not yet expired" range queries impossible.
            put("expires_at", expiresAt() ?: NO_EXPIRATION)
        }

    /**
     * The document a put feeds, `{"fields":{…}}`, written in one pass: the same
     * text `buildJsonObject { put("fields", indexFields(…)) }.toString()` gives
     * (pinned by `JsonCharsTest`), without building the tree. [indexFields] stays
     * the readable spec and what tests and the mock compare against.
     */
    fun feedJson(
        includeNear: Boolean = true,
        includeEdge: Boolean = false,
        includeStem: Boolean = true,
    ): String {
        val tagChars = tags.sumOf { tag -> 2 + tag.sumOf { it.length + 5 } }
        val out = JsonChars(FEED_OVERHEAD + content.length + 2 * tagChars)
        out.raw("{\"fields\":{")
        out.key("id", first = true).string(id)
        out.key("pubkey").string(pubkey)
        out.key("created_at").raw(createdAt)
        out.key("kind").raw(kind.toLong())
        out.key("tags").tagsString(tags)
        out.key("tag_index").raw('[')
        var firstIndexed = true
        forEachIndexedTag { name, value ->
            if (!firstIndexed) out.raw(',')
            firstIndexed = false
            out.pair(name, value)
        }
        out.raw(']')
        out.key("content").string(content)
        out.key("sig").string(sig)
        out.key("owner").string(owner)
        out.key("author_ref").string("id:reputation:reputation::$pubkey")
        for ((field, value) in search.fields()) out.key(field).string(value)
        if (includeNear) {
            for ((field, elements) in search.nearFieldsWritten()) out.key(field).strings(elements)
        }
        if (includeEdge) {
            for ((field, elements) in search.edgeFields()) out.key(field).strings(elements)
        }
        if (includeStem) {
            for ((field, value) in search.stemFields()) out.key(field).string(value)
        }
        out.key("expires_at").raw(expiresAt() ?: NO_EXPIRATION)
        return out.raw("}}").toString()
    }

    /** The complete NIP-01 event JSON, rebuilt from the exact stored values via Quartz's canonical serializer. */
    fun toEventJson(): String = Event(id, pubkey, createdAt, kind, tagsArray(), content, sig).toJson()

    /**
     * This doc as a Quartz [RawEvent] (`tags` kept as its canonical JSON
     * string), spliced verbatim by a relay's serializer — no per-tag object
     * built or re-serialized. The Vespa client builds RawEvents from decoded
     * summaries directly, skipping even this EventDoc.
     */
    fun toRawEvent(): RawEvent = RawEvent(id, pubkey, createdAt, kind, tagsJson(), content, sig)

    /** [tags] as the JSON text the `tags` field stores: `[["p","…"],…]`, escaped as kotlinx escapes it. */
    fun tagsJson(): String = JsonChars(tags.sumOf { tag -> 2 + tag.sumOf { it.length + 3 } } + 2).tags(tags).toString()

    companion object {
        /** The `expires_at` value for an event with no NIP-40 expiration: far enough out to outlive every range check. */
        const val NO_EXPIRATION = Long.MAX_VALUE

        /** [feedJson]'s keys, the fixed-width values and the quoting, so the buffer rarely grows. */
        private const val FEED_OVERHEAD = 600

        /**
         * `created_at desc, id asc` — the ONE order this store defines over
         * events, and deliberately a single comparator because its two uses are
         * the same rule read from opposite ends: it is the NIP-01 SERVING order
         * (newest first, ties by lowest id) and, taking the minimum instead of
         * iterating, the NIP-01 replaceable WINNER (highest created_at, ties to
         * the lowest id). Every recall path — the reference index, the Vespa
         * client's client-side tiebreak, the store's cross-filter merge — sorts
         * with this, so a change here moves all of them together.
         */
        val NEWEST_FIRST: Comparator<EventDoc> = compareByDescending<EventDoc> { it.createdAt }.thenBy { it.id }

        /** Parse a raw NIP-01 event JSON into a doc (Quartz's parser). Throws on a malformed event. */
        fun fromEventJson(raw: String): EventDoc =
            Event.fromJson(raw).let { e ->
                EventDoc(
                    id = e.id,
                    pubkey = e.pubKey,
                    createdAt = e.createdAt,
                    kind = e.kind,
                    tags = e.tags.map { it.toList() },
                    content = e.content,
                    sig = e.sig,
                )
            }

        /** Parse a Vespa summary/visit `fields` object (the [indexFields] shape) back into a doc. */
        fun fromSummary(fields: JsonObject): EventDoc {
            val pubkey = fields.getValue("pubkey").jsonPrimitive.content
            return EventDoc(
                id = fields.getValue("id").jsonPrimitive.content,
                pubkey = pubkey,
                createdAt = fields.getValue("created_at").jsonPrimitive.long,
                kind = fields.getValue("kind").jsonPrimitive.int,
                tags =
                    Json
                        .parseToJsonElement(fields.getValue("tags").jsonPrimitive.content)
                        .jsonArray
                        .map { tag -> tag.jsonArray.map { it.jsonPrimitive.content } },
                content = fields["content"]?.jsonPrimitive?.content ?: "",
                // Vespa OMITS empty-string fields from summaries: an unsigned
                // rumor (sig == "") arrives with no `sig` key — default to ""
                // instead of letting getValue throw and silently drop the hit.
                sig = fields["sig"]?.jsonPrimitive?.content ?: "",
                owner = fields["owner"]?.jsonPrimitive?.content ?: pubkey,
                search = SearchFields.fromFields { fields[it]?.jsonPrimitive?.content },
            )
        }
    }
}
