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
package com.nosfabrica.vespa.eventstore.engine.client

import com.nosfabrica.vespa.eventstore.engine.doc.EventDoc
import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.vitorpamplona.quartz.nip01Core.store.RawEvent
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive

/**
 * The wire shapes Vespa's document/search/visit APIs answer with, decoded as
 * flat DTOs rather than a JsonElement tree — the recall path is hot, and a
 * typed decode allocates the target objects directly.
 */

internal const val EVENT_NAMESPACE = "event"
internal const val EVENT_DOCTYPE = "event"

/** Lenient decoder: responses carry documentid/sddocname and other fields we don't model. */
internal val VESPA_JSON = Json { ignoreUnknownKeys = true }

/** Newest first (`created_at` desc, id asc tiebreak) over raw summaries — the client applies the id tiebreak, not the engine. */
internal val SUMMARY_NEWEST_FIRST = compareByDescending(VespaSummary::createdAt).thenBy(VespaSummary::id)

/** `/search/` response: the hit children plus [SearchCoverage]; grouping/meta are ignored. */
@Serializable
internal class SearchEnvelope(
    val root: SearchRoot = SearchRoot(),
)

@Serializable
internal class SearchRoot(
    val children: List<SearchHit> = emptyList(),
    val coverage: SearchCoverage = SearchCoverage(),
    val fields: SearchRootFields = SearchRootFields(),
) {
    /**
     * [EventQuery.complete]'s check, on top of [SearchCoverage.requireComplete]:
     * the engine must call the answer `full` — the exact `docs == active`,
     * which the read path deliberately does NOT key on (see
     * [SearchCoverage.undegraded]) — and must have served every match it
     * counted. Refuses the two shorter-than-it-looks shapes that pass the
     * read path's guard: the not-full-at-a-rounded-100 answer of a node still
     * opening buckets, and the hits-capped answer of a deployment whose query
     * profile caps `maxHits` (`VESPA_UNBOUNDED_HITS`). Loud on purpose: the
     * callers that ask for this derive and delete from what they read, and a
     * retry after the engine settles costs nothing while a wrong write costs
     * a document.
     */
    fun requireEverything() {
        require(coverage.full) {
            "vespa answered full: false (${coverage.coverage}% of the corpus, degraded: ${coverage.degraded ?: "unspecified"}) to a read that " +
                "must see everything — a node still opening its buckets, most likely; refused rather than derived from"
        }
        require(fields.totalCount <= children.size) {
            "vespa served ${children.size} of ${fields.totalCount} matches to a read that must see everything — the deployed hits cap " +
                "(the query profile's maxHits / VESPA_UNBOUNDED_HITS) is below this match set; refused rather than derived from"
        }
    }
}

/**
 * The response's own summary line. [totalCount] is HOW MANY DOCUMENTS THE QUERY
 * WOULD HAVE SERVED — not how many it returned, and, on a profile that drops
 * hits (the trust gate's `rank-score-drop-limit`), not how many merely matched
 * either: a dropped hit is gone from this number too. That is what makes it an
 * answer to NIP-45 rather than a diagnostic, and what lets a count ask for ZERO
 * hits (VespaEventIndex.count).
 *
 * MEASURED against a real Vespa (2026-09-01, 360k real events, a 65k-pubkey web
 * of trust): `totalCount` equalled the number of documents the same query
 * actually served at every `min_rank` from 0 to 95 — 12,215 ungated, 4,950 at
 * floor 20, 530 at floor 95 — while costing 39 ms against 4,559 ms to serve the
 * documents and count them client-side.
 *
 * It is NOT trustworthy under a match phase, which caps it (10x+ undercount);
 * see EventYql.countProfileOf, which is what keeps those profiles away from it.
 */
@Serializable
internal class SearchRootFields(
    val totalCount: Int = 0,
)

/**
 * How much of the corpus the engine actually searched. Vespa degrades rather
 * than failing — a query it gives up on returns HTTP 200 with fewer hits and
 * `full: false` — so this is the only thing separating "everything that
 * matched" from "some of it". Defaults to complete: responses with no coverage
 * block (document gets) have nothing to degrade.
 */
@Serializable
internal class SearchCoverage(
    /**
     * `docs == active` as the engine computes it. Decoded for the failure
     * message — a refused response that calls itself full is exactly the one
     * worth seeing — and deliberately NOT part of the verdict: see [undegraded].
     */
    val full: Boolean = true,
    val coverage: Int = 100,
    val nodes: Int = 1,
    val degraded: JsonObject? = null,
) {
    /** The engine cut the match phase — the one degradation the recall path may act on rather than refuse. */
    val matchPhaseDegraded: Boolean
        get() = (degraded?.get("match-phase") as? JsonPrimitive)?.content == "true"

    /**
     * The engine reports complete coverage and names no degradation — the whole
     * question this guard asks, and deliberately NOT [full].
     *
     * `full` and [coverage] come from different denominators: `full` is the
     * exact `docs == active`, the percentage rounds `docs / targetActive`. They
     * disagree in both directions, and keying on `full` got both wrong. A node a
     * hair short of its target (mid-redistribution, or still opening buckets
     * after a restart) is `full: false` at a rounded 100% with no `degraded`
     * block at all — refusing that failed EVERY query, with nothing to act on
     * and a rerun that comes back the same. Conversely `docs == active` while
     * both sit BELOW targetActive is `full: true` at any percentage, with
     * `non-ideal-state` in the block — served silently. Asking Vespa's own
     * question answers both spellings.
     *
     * The residual is bounded by the rounding: at 100% at most 0.5% of the
     * target went unsearched, so a page may under-deliver and a NIP-45 count
     * read that much low. The write-path guards pay it too, since dedup and the
     * NIP-09/62 probes come through here. The alternative is a store that
     * answers nothing while a node finishes settling.
     */
    val undegraded: Boolean
        get() = coverage >= 100 && degraded == null

    /**
     * A degraded response is a WRONG answer, not a slow one: at every call site
     * it is indistinguishable from a filter that genuinely matched that few,
     * and the dedup/NIP-09/62 guards decide by "did the query find it" — a
     * partial answer could resurrect a deleted event. So it fails loudly.
     * One exception: [allowMatchPhase], for the match-phase profiles that ASK
     * for the cut and verify or rerun the page themselves. The question asked is
     * Vespa's own `isDegraded()`, not its `full` — see [undegraded].
     */
    fun requireComplete(allowMatchPhase: Boolean = false) {
        if (undegraded) return
        // Vespa lists every degradation flag, false ones included — judge by
        // the flags actually SET.
        val set = degraded?.mapValues { (it.value as? JsonPrimitive)?.content == "true" }.orEmpty()
        val onlyMatchPhase = set["match-phase"] == true && set.none { (flag, on) -> on && flag != "match-phase" }
        require(allowMatchPhase && onlyMatchPhase) {
            // `full` rides along because a refused response that calls itself
            // full is the confusing one, and naming the contradiction beats
            // making the next reader rediscover the two denominators.
            "vespa searched only $coverage% of the corpus (full: $full, degraded: ${degraded ?: "unspecified"}); " +
                "the response is a PARTIAL answer, not a small one, so it is refused rather than returned"
        }
    }
}

@Serializable
internal class SearchHit(
    val fields: VespaSummary? = null,
    val relevance: Double = 0.0,
)

/** `/document/v1/…` get response. */
@Serializable
internal class DocEnvelope(
    val fields: VespaSummary? = null,
)

/** One projected doc off a visit page or stream — the lean carrier the visit walk hands its consumer. */
internal class VisitedDoc(
    val id: String,
    val fields: VisitFields?,
)

/** The projected fields the walks ask for (`created_at[,tag_index]` / `pubkey` / `tags`); everything else is server-trimmed. */
@Serializable
internal class VisitFields(
    /** The event id — projected by the id walks, since the docid is NOT the event id for an address-keyed replaceable. */
    val id: String? = null,
    @SerialName("created_at") val createdAt: Long? = null,
    @SerialName("tag_index") val tagIndex: List<String>? = null,
    val pubkey: String? = null,
    /** The stored tag JSON string (the tags projection), decoded by the caller. */
    val tags: String? = null,
)

/** A paged visit response: projected docs plus the continuation. */
@Serializable
internal class PagedVisitEnvelope(
    val documents: List<PagedVisitDoc> = emptyList(),
    val continuation: String? = null,
)

@Serializable
internal class PagedVisitDoc(
    val id: String = "",
    val fields: VisitFields? = null,
)

/** One JSON-Lines stream line: exactly one of put/continuation/message is set; anything else (sessionStats) decodes all-null. */
@Serializable
internal class StreamLine(
    val put: String? = null,
    val fields: VisitFields? = null,
    val continuation: StreamContinuation? = null,
    val message: StreamMessage? = null,
)

@Serializable
internal class StreamContinuation(
    val token: String? = null,
)

@Serializable
internal class StreamMessage(
    val severity: String? = null,
    val text: String? = null,
)

/** A `[document]` visit page: full summaries plus the continuation. */
@Serializable
internal class DocVisitEnvelope(
    val documents: List<DocVisitDoc> = emptyList(),
    val continuation: String? = null,
)

@Serializable
internal class DocVisitDoc(
    val fields: VespaSummary? = null,
)

/**
 * The summary a hit/doc carries. The recall path's trimmed select returns only
 * the NIP-01 fields, so the search columns decode as null there
 * (-> SearchFields.NONE); a full document get carries them, keeping get() lossless.
 */
@Serializable
internal class VespaSummary(
    val id: String = "",
    val pubkey: String = "",
    @SerialName("created_at") val createdAt: Long = 0,
    val kind: Int = 0,
    val tags: String = "[]",
    /** Only the `idtimetag` summary projects this — see [EventYql.buildIdTime]. */
    @SerialName("tag_index") val tagIndex: List<String>? = null,
    val content: String = "",
    val sig: String = "",
    val owner: String? = null,
    val name: String? = null,
    @SerialName("display_name") val displayName: String? = null,
    val about: String? = null,
    val nip05: String? = null,
    val lud16: String? = null,
    val website: String? = null,
    @SerialName("search_primary") val primary: String? = null,
    @SerialName("search_secondary") val secondary: String? = null,
    @SerialName("search_text") val text: String? = null,
    @SerialName("search_location") val location: String? = null,
    // The near-tier attribute arrays, present only on document-API reads (no
    // `| summary` in the schema). Decoded solely to stamp
    // EventDoc.storedNearFields — the reindex's evidence of whether this doc
    // predates the near tier.
    @SerialName("name_near") val nameNear: List<String>? = null,
    @SerialName("search_primary_near") val primaryNear: List<String>? = null,
    @SerialName("search_secondary_tokens") val secondaryTokens: List<String>? = null,
    @SerialName("affil_tokens") val affilTokens: List<String>? = null,
    /** The rank profile's declared match-features, when the profile has any (see event.sd). */
    val matchfeatures: JsonObject? = null,
) {
    /**
     * Rebuild a doc from the decoded summary. `tags` is the one field still
     * parsed per hit — the store keeps it as a JSON string.
     *
     * [withNearState] stamps [EventDoc.storedNearFields] from the summary's
     * near arrays, and must be passed ONLY on document-API reads, where the
     * response is the complete stored document. Search summaries never carry
     * the near fields, so stamping there would claim every doc predates the
     * near tier.
     */
    fun toDoc(withNearState: Boolean = false): EventDoc? {
        if (id.isEmpty()) return null
        return EventDoc(
            id = id,
            pubkey = pubkey,
            createdAt = createdAt,
            kind = kind,
            tags = Json.parseToJsonElement(tags).jsonArray.map { row -> row.jsonArray.map { it.jsonPrimitive.content } },
            content = content,
            sig = sig,
            owner = owner ?: pubkey,
            // normalized(): the feed writes "" for an absent name/display_name
            // sibling, so fold it back to null on decode.
            search = SearchFields(name, displayName, about, nip05, lud16, website, primary, secondary, text, location).normalized(),
        ).also { if (withNearState) it.storedNearFields = nearArrays() }
    }

    /**
     * The summary as a [RawEvent] with NO tag parse: `tags` rides through as
     * the exact JSON string Vespa stored, which is precisely what a relay's
     * serializer splices after `"tags":` — the whole raw-recall win.
     */
    fun toRaw(): RawEvent? {
        if (id.isEmpty()) return null
        return RawEvent(id, pubkey, createdAt, kind, tags, content, sig)
    }

    /** The near arrays this summary carries, keyed as fed. Only meaningful on document-API reads (see [toDoc]). */
    fun nearArrays(): Map<String, List<String>> =
        buildMap {
            nameNear?.let { put("name_near", it) }
            primaryNear?.let { put("search_primary_near", it) }
            secondaryTokens?.let { put("search_secondary_tokens", it) }
            affilTokens?.let { put("affil_tokens", it) }
        }
}
