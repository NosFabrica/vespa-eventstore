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

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.nio.file.Files
import java.nio.file.Path

/**
 * The graded relevance judgments ("qrels") the search-quality measures are
 * computed against: `benchmark/search_judgments.json`, keyed by TOPIC (a
 * `rank_cases.json` row's `id`, else its query text), then by event id —
 *
 *     { "Avi Burra": { "672c8192…": { "grade": 3, "by": "human:vitor", "at": "2026-10-23" } } }
 *
 * A bare integer is accepted for a grade (hand-written rows); `rankAb --merge`
 * always writes the object form, because WHO judged is part of the datum: the
 * protocol lets an LLM pre-judge a pool only if a human audits a sample, and
 * that audit needs to find the LLM's rows (`docs/search-evaluation.md` §5).
 * Keys starting with `_` are documentation and are skipped.
 *
 * Separate from `rank_cases.json` on purpose (TREC keeps topics and qrels
 * apart for the same reason): the case file is curated prose that people
 * edit by hand, this file is tool-managed and grows by hundreds of rows a
 * round, and a rewrite of one must never reformat the other.
 *
 * An event id may be given as a PREFIX (≥ 8 hex chars, like `expect`); the
 * longest matching key wins.
 */
class Judgments private constructor(
    private val topics: LinkedHashMap<String, LinkedHashMap<String, Judgment>>,
) {
    data class Judgment(
        val grade: Int,
        val by: String? = null,
        val at: String? = null,
    )

    val topicCount: Int get() = topics.size

    /** Every grade the topic has — the pool its ideal ranking is built from. */
    fun grades(topic: String): List<Int> = topics[topic]?.values?.map { it.grade }.orEmpty()

    /** How many documents the topic has judged. */
    fun count(topic: String): Int = topics[topic]?.size ?: 0

    fun gradeOf(
        topic: String,
        id: String,
    ): Int? =
        topics[topic]?.let { judged ->
            judged[id]?.grade ?: judged.entries
                .filter { id.startsWith(it.key) }
                .maxByOrNull { it.key.length }
                ?.value
                ?.grade
        }

    fun put(
        topic: String,
        id: String,
        judgment: Judgment,
    ) {
        require(judgment.grade in SCALE) { "grade ${judgment.grade} for $topic/$id is outside $SCALE" }
        require(id.length >= MIN_PREFIX && id.all { it in HEX }) { "'$id' is not an event id (or a ≥ $MIN_PREFIX-char hex prefix of one)" }
        topics.getOrPut(topic) { LinkedHashMap() }[id] = judgment
    }

    /**
     * A copy carrying [prefix] at [grade] unless the topic already judges a
     * matching id — how a case's `expect` (a human report, so trusted) becomes
     * a judgment without anyone transcribing it.
     */
    fun withImplicit(implicit: Map<String, Pair<String, Int>>): Judgments {
        val copy = Judgments(LinkedHashMap(topics.mapValues { LinkedHashMap(it.value) }))
        for ((topic, judgment) in implicit) {
            val (prefix, grade) = judgment
            if (copy.topics[topic]?.keys?.none { it.startsWith(prefix) || prefix.startsWith(it) } != false) {
                copy.put(topic, prefix, Judgment(grade, by = "expect"))
            }
        }
        return copy
    }

    /** Rewrites [path], topics and ids in insertion order, two-space indented like the repo's other JSON. */
    @OptIn(ExperimentalSerializationApi::class)
    fun write(path: Path) {
        val json =
            buildJsonObject {
                put("_doc", DOC)
                for ((topic, judged) in topics) {
                    put(
                        topic,
                        buildJsonObject {
                            for ((id, j) in judged) {
                                put(
                                    id,
                                    buildJsonObject {
                                        put("grade", j.grade)
                                        j.by?.let { put("by", it) }
                                        j.at?.let { put("at", it) }
                                    },
                                )
                            }
                        },
                    )
                }
            }
        Files.writeString(
            path,
            Json {
                prettyPrint = true
                prettyPrintIndent = "  "
            }.encodeToString(JsonObject.serializer(), json) + "\n",
        )
    }

    companion object {
        /** 0 not relevant · 1 marginal · 2 relevant · 3 the answer. Defined in docs/search-evaluation.md §4. */
        val SCALE = 0..3
        const val MIN_PREFIX = 8
        private val HEX = "0123456789abcdef".toSet()
        private const val DOC =
            "Graded relevance judgments for benchmark/rank_cases.json topics: topic -> event id -> grade (0 not relevant, " +
                "1 marginal, 2 relevant, 3 the answer). Protocol and grading guide: docs/search-evaluation.md. Tool-managed: " +
                "add rows with `rankAb --pool` + `rankAb --merge`, not by hand-editing at scale."

        fun empty(): Judgments = Judgments(LinkedHashMap())

        /** Loads [path]; a missing file is an empty set (every topic then reads as unjudged, which `rankAb` says). */
        fun load(path: Path): Judgments {
            if (!Files.exists(path)) return empty()
            val out = empty()
            for ((topic, judged) in Json.parseToJsonElement(Files.readString(path)).jsonObject) {
                if (topic.startsWith("_")) continue
                for ((id, v) in judged.jsonObject) {
                    val judgment =
                        if (v is JsonPrimitive) {
                            Judgment(v.int)
                        } else {
                            val o = v.jsonObject
                            Judgment(
                                grade = o.getValue("grade").jsonPrimitive.int,
                                by = o["by"]?.jsonPrimitive?.content,
                                at = o["at"]?.jsonPrimitive?.content,
                            )
                        }
                    out.put(topic, id, judgment)
                }
            }
            return out
        }
    }
}
