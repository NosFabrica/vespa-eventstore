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

import com.vitorpamplona.quartz.nip01Core.core.HexKey
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.double
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.putJsonObject

/**
 * One pubkey's ranking state: the `reputation` GLOBAL parent doc every event
 * imports for trust-weighted ranking (`author_ref`). NOT an event — the trust
 * projection derives it from stored kind-30382s and rewrites it whole on
 * any time. Tensor cells
 * key by SERVICE pubkey — the signer of the 30382: [influenceScores] = rank
 * (influence*100, 0..100), [followerCounts] = verified-follower count. An
 * observer reaches a cell by resolving their kind 10040 to the service key at
 * query time (EventQuery.rankKey / followersKey).
 */
data class ReputationDoc(
    val pubkey: HexKey,
    val influenceScores: Map<ServiceKey, Int> = emptyMap(),
    val followerCounts: Map<ServiceKey, Double> = emptyMap(),
) {
    /** No cells at all — the projection removes the doc instead of storing it. */
    fun isEmpty(): Boolean = influenceScores.isEmpty() && followerCounts.isEmpty()

    /** The document's field map (mapped tensors in Vespa's short object form). */
    fun indexFields(): JsonObject =
        buildJsonObject {
            put("pubkey", JsonPrimitive(pubkey))
            putJsonObject("influence_scores") { influenceScores.forEach { (service, rank) -> put(service.hex, JsonPrimitive(rank)) } }
            putJsonObject("follower_counts") { followerCounts.forEach { (service, count) -> put(service.hex, JsonPrimitive(count)) } }
        }

    companion object {
        /**
         * Parse a document-API `fields` object. Mapped tensors arrive in TWO
         * shapes: the short form we feed (`{obs: v}`) and the verbose
         * `{"type": …, "cells": …}` form document-API GETs render.
         */
        fun fromSummary(fields: JsonObject): ReputationDoc =
            ReputationDoc(
                pubkey = fields.getValue("pubkey").jsonPrimitive.content,
                influenceScores = cells(fields["influence_scores"])?.map { ServiceKey(it.key) to it.value.jsonPrimitive.int }?.toMap() ?: emptyMap(),
                followerCounts = cells(fields["follower_counts"])?.map { ServiceKey(it.key) to it.value.jsonPrimitive.double }?.toMap() ?: emptyMap(),
            )

        private fun cells(field: JsonElement?): Map<String, JsonElement>? = field?.jsonObject?.let { it["cells"]?.jsonObject ?: it }
    }
}

/**
 * One score card's contribution to [subject]'s parent: the cells under [key]
 * — the SERVICE KEY that signed the card — applied as a partial UPDATE: no
 * read, no full-doc rewrite. Null fields leave the corresponding tensor
 * untouched.
 */
data class ReputationCells(
    val subject: HexKey,
    val key: ServiceKey,
    val influence: Int?,
    val followers: Double?,
    /**
     * Retraction, in the same update: the card no longer carries the tag, so
     * the cell it used to back is removed (a tensor `remove`) as the other
     * dimension's cell is written. One document update per card, atomic.
     */
    val dropInfluence: Boolean = false,
    val dropFollowers: Boolean = false,
)

/**
 * The cells a removed or retracted card leaves behind: [subject]'s cells under
 * [key] (the signing service), per dimension. Applied as a tensor `remove`, no
 * read; a cell that is not there is nothing to do.
 */
data class CellRemoval(
    val subject: HexKey,
    val key: ServiceKey,
    val influence: Boolean,
    val followers: Boolean,
)
