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
    fun tagIndex(): List<String> =
        tags.mapNotNull { tag ->
            val name = tag.getOrNull(0) ?: return@mapNotNull null
            val value = tag.getOrNull(1) ?: return@mapNotNull null
            if (isIndexableTagName(name)) "$name:$value" else null
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

    /** The document's field map — one shape for both feeding and summary parsing ([fromSummary]). */
    fun indexFields(): JsonObject =
        buildJsonObject {
            put("id", id)
            put("pubkey", pubkey)
            put("created_at", createdAt)
            put("kind", kind)
            put("tags", tagsAsJson().toString())
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
            for ((field, elements) in search.nearFieldsWritten()) {
                put(field, JsonArray(elements.map(::JsonPrimitive)))
            }
            // Always written. An absent numeric attribute reads as 0 in Vespa,
            // which would make "not yet expired" range queries impossible.
            put("expires_at", expiresAt() ?: NO_EXPIRATION)
        }

    /** The complete NIP-01 event JSON, rebuilt from the exact stored values via Quartz's canonical serializer. */
    fun toEventJson(): String = Event(id, pubkey, createdAt, kind, tagsArray(), content, sig).toJson()

    /**
     * This doc as a Quartz [RawEvent] (`tags` kept as its canonical JSON
     * string), spliced verbatim by a relay's serializer — no per-tag object
     * built or re-serialized. The Vespa client builds RawEvents from decoded
     * summaries directly, skipping even this EventDoc.
     */
    fun toRawEvent(): RawEvent = RawEvent(id, pubkey, createdAt, kind, tagsAsJson().toString(), content, sig)

    private fun tagsAsJson(): JsonArray = JsonArray(tags.map { tag -> JsonArray(tag.map(::JsonPrimitive)) })

    companion object {
        /** The `expires_at` value for an event with no NIP-40 expiration: far enough out to outlive every range check. */
        const val NO_EXPIRATION = Long.MAX_VALUE

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
