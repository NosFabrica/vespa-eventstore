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
package com.vitorpamplona.quartz.eventstore.vespa.doc
import com.vitorpamplona.quartz.eventstore.vespa.isSingleLetterTagName
import com.vitorpamplona.quartz.nip01Core.core.Address
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.core.isAddressable
import com.vitorpamplona.quartz.nip01Core.core.isReplaceable
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
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
 * One live Nostr event as a Vespa `event` document (docid = [id]). The NIP-01
 * fields are held LOSSLESSLY. The signature is over the canonical serialization
 * of these exact values, so [toEventJson] can rebuild a complete event that
 * clients re-verify. There is no separate raw-blob copy.
 *
 * [tags] is the exact tag array. The queryable `tag_index` field ([tagIndex])
 * is a derived, lossy view: single-letter tag names only, first value only. It
 * is used for `#x` filter recall and never for reconstruction.
 *
 * The store maps its events into this and verifies signatures BEFORE
 * constructing one, so everything in the index is assumed already verified.
 * NIP-01 serialization and the tag-derived reads go through the Quartz [Event]
 * this doc reconstructs ([toEventJson], [expiresAt], [dTag]).
 *
 * [owner] and [search] are DERIVED fields the store computes with Nostr
 * knowledge. The owner is the pubkey Nostr semantics key off: the gift-wrap
 * recipient for kind 1059, else the author. [search] is the kind-specific
 * decomposition of searchable kinds into the schema's search fields. All-null
 * means the event is invisible to NIP-50 search.
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
     * actually STORED on the engine document this instance was decoded from.
     * null = no evidence — constructed docs, and every search-summary path
     * (the near fields carry no `| summary`, so their absence there says
     * nothing). Only the document-API reads (get, the `[document]` visit) see
     * the true stored fields and stamp this; an empty map from those paths
     * means the doc genuinely holds NO near arrays — the pre-near-tier corpus
     * the full-text reindex must re-feed ([SearchFields.nearFieldsWritten] is
     * what a fresh put would write, so `stored != written` = stale).
     *
     * Deliberately NOT a constructor property: a doc's identity is the event
     * plus its derived search surface, and two reads of the same logical doc
     * (one via search, one via visit) must stay equal — this records what one
     * read happened to see, so it stays outside equals/copy.
     */
    var storedNearFields: Map<String, List<String>>? = null

    /** [tags] as Quartz's `String[][]` shape, for its tag-array helpers and event reconstruction. */
    private fun tagsArray(): Array<Array<String>> = Array(tags.size) { tags[it].toTypedArray() }

    /**
     * The queryable `"<letter>:<value>"` pairs. One per tag whose name is a
     * single ASCII letter (the names NIP-01 `#x` filters can address) and that
     * has a value. Everything else still round-trips through [tags].
     */
    fun tagIndex(): List<String> =
        tags.mapNotNull { tag ->
            val name = tag.getOrNull(0) ?: return@mapNotNull null
            val value = tag.getOrNull(1) ?: return@mapNotNull null
            if (isSingleLetterTagName(name)) "$name:$value" else null
        }

    /**
     * The NIP-40 expiration timestamp; null = never expires. A direct scan over
     * [tags] — same result as Quartz's `String[][].expiration()` (first
     * `expiration` tag with a parseable value) without the per-call `String[][]`
     * deep copy [tagsArray] would make. Called on every put (see [indexFields]).
     */
    fun expiresAt(): Long? =
        tags.firstNotNullOfOrNull { tag ->
            if (tag.size > 1 && tag[0] == "expiration" && tag[1].isNotEmpty()) tag[1].toLongOrNull() else null
        }

    /**
     * The `d` tag value, or "" when absent — the addressable bucketing key
     * (missing == empty). A direct scan over [tags], matching Quartz's
     * `String[][].dTag()` (`firstTagValue("d") ?: ""`) with no `String[][]` copy;
     * this is on the address/supersession hot path.
     */
    fun dTagOrEmpty(): String = tags.firstOrNull { it.size > 1 && it[0] == "d" }?.get(1) ?: ""

    /** The first `d` tag's value — an addressable event's identity key; null/blank = none. */
    fun dTag(): String? = dTagOrEmpty().takeIf { it.isNotEmpty() }

    /**
     * The NIP-01 address (`kind:pubkey[:dtag]`) for replaceable/addressable
     * kinds; null for regular events. This is the natural unique key those kinds
     * supersede on — and, under address-keyed storage, their document id.
     * Replaceables use the fixed empty d-tag; addressables use their `d` value.
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
            // Reference to the author's ranking state (the global reputation
            // parent), which controls how any kind ranks by the observer's
            // trust in its author. It is purely pubkey-derived, so it's stamped
            // here rather than by an extractor.
            put("author_ref", "id:reputation:reputation::$pubkey")
            for ((field, value) in search.fields()) put(field, value)
            // The near-tier attribute arrays (prefix/fuzzy targets), derived
            // HERE rather than by a schema-side indexing expression so doc and
            // query share NearText's one normalization (diacritic folding —
            // string attributes match raw bytes) — see NearText for the full
            // rationale. Existing corpora need a RE-FEED to populate these;
            // NostrSemanticsStore.reindexFullTextSearch is that re-feed (it
            // compares [storedNearFields] against this derivation).
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
     * This doc as a Quartz [RawEvent]: the wire event with `tags` kept as its
     * canonical JSON string. A raw recall path hands this straight to a relay's
     * serializer (which splices `jsonTags` verbatim), so no per-tag object is
     * ever built or re-serialized. The Vespa client overrides its recall to build
     * the RawEvent from the decoded summary directly, skipping even this EventDoc.
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
                // Tolerant like `content` above: Vespa OMITS empty-string fields from
                // query summaries, so an unsigned rumor (sig == "") comes back with no
                // `sig` key at all. getValue would throw and the hit would silently
                // vanish from search results. Default to "" instead.
                sig = fields["sig"]?.jsonPrimitive?.content ?: "",
                owner = fields["owner"]?.jsonPrimitive?.content ?: pubkey,
                search = SearchFields.fromFields { fields[it]?.jsonPrimitive?.content },
            )
        }
    }
}
