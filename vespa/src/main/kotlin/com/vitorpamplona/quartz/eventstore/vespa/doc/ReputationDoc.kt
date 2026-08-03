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
 * One pubkey's ranking state: the `reputation` GLOBAL parent document every event
 * references (`author_ref`) and imports for trust-weighted ranking. It is NOT
 * an event. The trust projection derives it from stored kind-30382s and
 * rewrites it whole on every change (recompute, not cell surgery), so it can be
 * rebuilt from the event corpus at any time.
 *
 * Tensor cells are keyed by OBSERVER pubkey: [influenceScores] = rank
 * (influence*100, 0..100), [followerCounts] = verified-follower count.
 */
data class ReputationDoc(
    val pubkey: HexKey,
    val influenceScores: Map<HexKey, Int> = emptyMap(),
    val followerCounts: Map<HexKey, Double> = emptyMap(),
) {
    /** No cells at all — the projection removes the doc instead of storing it. */
    fun isEmpty(): Boolean = influenceScores.isEmpty() && followerCounts.isEmpty()

    /** The document's field map (mapped tensors in Vespa's short object form). */
    fun indexFields(): JsonObject =
        buildJsonObject {
            put("pubkey", JsonPrimitive(pubkey))
            putJsonObject("influence_scores") { influenceScores.forEach { (observer, rank) -> put(observer, JsonPrimitive(rank)) } }
            putJsonObject("follower_counts") { followerCounts.forEach { (observer, count) -> put(observer, JsonPrimitive(count)) } }
        }

    companion object {
        /**
         * Parse a document-API `fields` object back into a doc. Mapped tensors
         * arrive in TWO shapes: the short object form we feed (`{obs: v}`) and
         * the verbose form document-API GETs render (`{"type": …, "cells": {obs: v}}`).
         */
        fun fromSummary(fields: JsonObject): ReputationDoc =
            ReputationDoc(
                pubkey = fields.getValue("pubkey").jsonPrimitive.content,
                influenceScores = cells(fields["influence_scores"])?.mapValues { it.value.jsonPrimitive.int } ?: emptyMap(),
                followerCounts = cells(fields["follower_counts"])?.mapValues { it.value.jsonPrimitive.double } ?: emptyMap(),
            )

        private fun cells(field: JsonElement?): Map<String, JsonElement>? = field?.jsonObject?.let { it["cells"]?.jsonObject ?: it }
    }
}

/**
 * One score card's contribution to [subject]'s parent doc: the [observer]'s
 * cells, applied as a partial UPDATE with no read and no full-doc rewrite. Null
 * fields leave the corresponding tensor untouched.
 */
data class ReputationCells(
    val subject: String,
    val observer: String,
    val influence: Int?,
    val followers: Double?,
)
