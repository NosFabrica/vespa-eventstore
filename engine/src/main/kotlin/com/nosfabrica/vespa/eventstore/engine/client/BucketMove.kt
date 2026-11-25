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

import ai.vespa.feed.client.DocumentId
import ai.vespa.feed.client.OperationParameters
import ai.vespa.feed.client.Result
import com.nosfabrica.vespa.eventstore.engine.app.AuthorShards
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.net.URLEncoder
import java.time.Duration
import java.util.TreeSet
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong

/**
 * PROTOTYPE: moving author buckets onto NEW content clusters while the store keeps serving
 * (`docs/author-shards.md`, "Moving buckets"). Vespa never moves a document between clusters,
 * so a move is a copy, a check, a switch of who serves, and a delete — each step measured to
 * behave as used here (Vespa 8, `AuthorShardsMoveIT`):
 *
 *  1. Every store switches to [overlap]: the moving buckets keep their owner and are also
 *     written to their destination, which serves nothing yet and so is PENDING — read by no one.
 *     Then the overlap package deploys; it only widens what clusters keep, so the deploy guard
 *     lets it through over data. From here on every new write lands in both clusters.
 *  2. [copy]: each moving bucket's events and reputation documents copied from the source
 *     cluster into the destination INSIDE Vespa (a document-API visit with `destinationCluster`).
 *     It overwrites, so a write racing it can be undone in the destination: step 3 is not optional.
 *  3. [diff] with repair, until clean: a write racing the copy can leave the destination wrong
 *     (a cell update overwritten by the older copy, a document partly created by a cell update,
 *     a delete undone by the copy). The source is complete and keeps receiving every write,
 *     so it is the reference: what the destination lacks or holds differently is copied, what it
 *     holds and the source does not is removed (`route=<destination>`).
 *  4. Every store switches to [to]: the destinations serve. The sources still hold copies, so a
 *     read naming both may meet a document twice; a sharded client drops repeated hits by id
 *     (and asks again for a page that shrank), and a COUNT over-reports for that window.
 *  5. [finish], in this order:
 *     - the moved documents are removed from their sources, by a document-API `DELETE` by
 *       selection, BEFORE the final package narrows them. The overlap package is still active,
 *       so a source keeps receiving every write to the moved buckets, and any copy a read
 *       meets there is current. Removing after the narrowing (the first cut) left the sources
 *       holding copies no write reached any more, for the whole cleanup: a delete in that window
 *       was served again from the old copy, and a moved author was gated on stale trust.
 *     - the final package deploys ([finish]'s caller). It NARROWS the sources, which the guard
 *       refuses unless told the copy was verified (`SchemaDeployer.checkShardChange(allowNarrowing = true)`).
 *     - a second removal takes what was written to the sources between the two, the only copies
 *       that can be stale, and only until it finishes. [remaining] confirms none are left.
 *
 *     A [diff] after [finish] has begun is refused: with the sources emptied it would take the
 *     moved documents for extras and delete them from their new owner.
 *
 * Holds, per batch of buckets, the destination's ids (and reputation documents) in memory during
 * [diff], batches sized to a fixed budget from what [copy] counted. Copy and cleanup run inside
 * Vespa; only [diff]'s repairs go through the feed client, one document at a time.
 */
class BucketMove(
    baseUrl: String,
    /** The map the deployment runs now, at rest. */
    val from: AuthorShards.BucketMap,
    /** The map it moves to, at rest; its new clusters receive the moving buckets. */
    val to: AuthorShards.BucketMap,
    /** Document-API containers; empty = just [baseUrl]. */
    endpoints: List<String> = emptyList(),
    tuning: ClientTuning = ClientTuning(),
    /** Documents one [fullDiff] batch may hold in memory; lowered only to measure the full diff at production's bucket sizes (`MoveSpeedProbe`). */
    private val diffBudget: Double = DIFF_BUDGET,
    /**
     * How far the content nodes' clocks may run behind this client's ([changedSince]): generous by
     * default, since it only widens the incremental diff's first pass. A probe that loads its corpus
     * just before moving it narrows it, or every document reads as changed.
     */
    private val clockMarginMs: Long = CLOCK_MARGIN_MS,
) : AutoCloseable {
    /** bucket -> (source cluster, destination cluster). */
    val moves: Map<Int, Pair<Int, Int>> = from.movesTo(to)

    /** The map while the move fills its destinations (step 1). */
    val overlap: AuthorShards.BucketMap = from.overlapTo(to)

    private val api = EndpointRotation(endpoints, baseUrl)

    private val http = VespaHttp()

    private val feed = VespaFeed(api.urls, eager = true, tuning = tuning)

    /** Documents an operation touched, per type. */
    class Counts(
        val events: Long,
        val reputations: Long,
    ) {
        override fun toString(): String = "events=$events reputations=$reputations"
    }

    /** How a destination differs from its source over the moving buckets. */
    class Diff(
        val missing: Long,
        val differing: Long,
        val extra: Long,
    ) {
        val clean: Boolean get() = missing == 0L && differing == 0L && extra == 0L

        operator fun plus(o: Diff): Diff = Diff(missing + o.missing, differing + o.differing, extra + o.extra)

        override fun toString(): String = "missing=$missing differing=$differing extra=$extra"
    }

    private class Type(
        val namespace: String,
        val doctype: String,
        /** What a selection tests the author by: the event's `pubkey` field, the reputation document's id. */
        val keyField: String,
        /** Compare whole documents in [diff] (reputation: its cells change in place), or ids only (events). */
        val compareFields: Boolean,
    )

    private val types =
        listOf(
            Type(EVENT_NAMESPACE, EVENT_DOCTYPE, "$EVENT_DOCTYPE.pubkey", compareFields = false),
            Type(REPUTATION, REPUTATION, "id.specific", compareFields = true),
        )

    /**
     * Step 2: the moving buckets' documents copied into their destinations INSIDE Vespa: a
     * document-API visit of the source with `destinationCluster`, which puts each selected
     * document into the destination from the content nodes, with no document passing through this
     * client. The first cut read every document here and put it back where absent; on one host
     * that moved ~2,600 documents/s while the host fed ~5,900 and visited ~13,500
     * (`MoveSpeedProbe`, `docs/author-shards.md`).
     *
     * A server-side copy OVERWRITES. A write that reached the destination after the copy read its
     * source can be replaced by the older version, and a delete can be undone. That is safe
     * only because [diff] runs after the copy, always: the source received every write and is the
     * reference, and the destination is pending, read by no one, until [diff] is clean.
     */
    suspend fun copy(): Counts {
        // Before the first document is read: anything written from here on may be what the copy
        // gets wrong, and [diff] reads it again.
        if (changedSince == null) changedSince = (System.currentTimeMillis() - clockMarginMs) * 1000
        return counts { type, buckets, src, dst ->
            val n = writeVisit(type, src, buckets, "&destinationCluster=${URLEncoder.encode(AuthorShards.clusterName(dst), "UTF-8")}", delete = false)
            // Sizes [diff]'s batches; a document API that reported no count leaves the conservative default.
            if (n > 0) seen[Triple(src, dst, type.doctype)] = n
            n
        }
    }

    /**
     * Step 3: how each destination differs from its source, counting only differences that
     * PERSIST. Under live writes two snapshots read seconds apart disagree on whatever was written
     * between them — and in the overlap every write lands in both clusters, so that is no
     * divergence. Each candidate is therefore read again from both clusters, point reads close
     * together, and counted (and, with [repair], fixed from the source's fresh read) only when it
     * still differs. A race can still slip past a pass; the next pass catches it.
     */
    suspend fun diff(
        repair: Boolean = false,
        /** Compare everything ([fullDiff]) even after a [copy]: a last complete check, or a measurement. */
        full: Boolean = false,
    ): Diff {
        check(!finishing) { "no diff once finish() has begun: the sources are being emptied, and a diff would delete the moved documents from their new owner" }
        val startedMs = System.currentTimeMillis()
        val since = changedSince
        val result = if (since == null || full) fullDiff(repair, pairs) else incrementalDiff(repair, since)
        // The next pass needs only what was written from this one's start on.
        if (since != null) changedSince = (startedMs - clockMarginMs) * 1000
        return result.also { lastDiffClean = it.clean }
    }

    /**
     * Vespa's document timestamps (microseconds) from which a source document may differ from its
     * copy: [copy]'s start, then each [diff]'s, less [CLOCK_MARGIN_MS] for the distance between
     * this client's clock and the content nodes'. Null before a [copy] (a diff of a move this
     * object did not copy), when [diff] compares everything.
     */
    @Volatile private var changedSince: Long? = null

    /**
     * The diff after a [copy], in passes that each read a cluster ONCE, where [fullDiff] re-reads
     * the source whole per batch of buckets (a visit evaluates its selection over every document)
     * and so could not finish at production size: ~128 batches of one ~3.7 M-document bucket each
     * for a two-cluster split of ~945 M events, each a full scan.
     *
     * Every way the copy goes wrong, measured on Vespa 8: the copy writes each document with a NEW
     * timestamp, so a write that reached the destination first is overwritten by the older copy,
     * and a delete that reached it first is undone (the document comes back). So:
     *  1. CHANGED: every source document written since [since] (`fromTimestamp`) is read in both
     *     clusters and settled ([settle]): what the copy left stale, partial or missing.
     *  2. DIGESTS: a delete leaves nothing to visit (`includeRemoves` is not a document-API visit
     *     parameter; measured: ignored), so what the copy undid is found by comparing, per author
     *     bucket, the two clusters' documents as a count and a sum of 64-bit hashes ([digests]),
     *     one streaming pass each: of ids for events, which never change in place, and of ids with
     *     their fields for reputation documents, so a write that reached the destination alone is
     *     found as well. Equal digests are equal sets but for a hash collision across a bucket (about
     *     2^-64 per bucket). A bucket whose digests differ gets [fullDiff], which settles every
     *     difference, and under live writes a bucket written between the two passes differs for
     *     nothing: settling finds nothing persistent, at the cost of one more full read of it.
     */
    private suspend fun incrementalDiff(
        repair: Boolean,
        since: Long,
    ): Diff {
        var total = Diff(0, 0, 0)
        for ((pair, all) in pairs) {
            val (src, dst) = pair
            for (type in types) {
                val changed = ConcurrentLinkedQueue<String>()
                visit(type, src, all, "[id]", "&fromTimestamp=$since") { docs -> docs.forEach { changed += idOf(it) } }
                total += settle(type, changed.toSet(), src, dst, repair)
            }
            val differing = TreeSet<Int>()
            for (type in types) {
                val (a, b) = coroutineScope { listOf(async { digests(type, src, all) }, async { digests(type, dst, all) }).awaitAll() }
                for (bucket in all) if (a[bucket] != b[bucket]) differing += bucket
            }
            if (differing.isNotEmpty()) total += fullDiff(repair, mapOf(pair to differing.toList()))
        }
        return total
    }

    /**
     * Per bucket of [buckets], [type]'s documents in [cluster] as (count, sum of 64-bit hashes), one
     * streaming pass: of each id, and for a type whose documents change in place (reputation) of
     * the id with its fields, so a document that differs only in content differs here too.
     */
    private suspend fun digests(
        type: Type,
        cluster: Int,
        buckets: List<Int>,
    ): Map<Int, Pair<Long, Long>> {
        val counts =
            java.util.concurrent.atomic
                .AtomicLongArray(256)
        val sums =
            java.util.concurrent.atomic
                .AtomicLongArray(256)
        // A reputation document changes in place, so its digest covers what it holds; an event never
        // does, so its id is enough, and its pubkey (what places it) is all that is read.
        val fieldSet = if (type.compareFields) "[document]" else "${type.doctype}:pubkey"
        visit(type, cluster, buckets, fieldSet) { docs ->
            for (d in docs) {
                val id = idOf(d)
                val key =
                    if (type.compareFields) {
                        id
                    } else {
                        (d["fields"] as? JsonObject)
                            ?.get("pubkey")
                            ?.jsonPrimitive
                            ?.content
                            .orEmpty()
                    }
                // A key with no hex bucket is cluster 0's (AuthorShards), never a moving bucket's.
                val bucket = AuthorShards.bucketOf(key) ?: continue
                counts.incrementAndGet(bucket)
                sums.addAndGet(bucket, idHash(if (type.compareFields) id + "\u0000" + canonical(d["fields"]) else id))
            }
        }
        return buckets.associateWith { counts.get(it) to sums.get(it) }
    }

    /** The diff that compares everything: [buckets]' documents read whole from both clusters, batch by batch. */
    private suspend fun fullDiff(
        repair: Boolean,
        buckets: Map<Pair<Int, Int>, List<Int>>,
    ): Diff {
        var total = Diff(0, 0, 0)
        for ((pair, all) in buckets) {
            val (src, dst) = pair
            // As few batches as the memory budget allows: each batch re-reads both clusters whole,
            // and a batch holds its destination's documents in memory. Sized from what copy()
            // counted; without that, the conservative BATCH.
            val known = types.mapNotNull { seen[Triple(src, dst, it.doctype)] }
            val batch =
                if (known.size < types.size) {
                    BATCH
                } else {
                    val perBucket = known.max().toDouble() / pairs.getValue(pair).size
                    (diffBudget / perBucket.coerceAtLeast(1.0)).toInt().coerceIn(1, all.size)
                }
            for (chunk in all.chunked(batch)) {
                for (type in types) {
                    val fieldSet = if (type.compareFields) "[document]" else "[id]"
                    val there = ConcurrentHashMap<String, String>()
                    visit(type, dst, chunk, fieldSet) { docs -> docs.forEach { there[idOf(it)] = canonical(it["fields"]) } }
                    val candidates = ConcurrentLinkedQueue<String>()
                    visit(type, src, chunk, fieldSet) { docs ->
                        for (d in docs) {
                            val id = idOf(d)
                            val theirs = there.remove(id)
                            if (theirs == null || (type.compareFields && theirs != canonical(d["fields"]))) candidates += id
                        }
                    }
                    candidates += there.keys
                    total += settle(type, candidates.toSet(), src, dst, repair)
                }
            }
        }
        return total
    }

    /**
     * [ids] read again from both clusters, point reads close together, and counted (and, with
     * [repair], fixed from the source's fresh read) only where they still differ: a write between
     * two snapshots is no divergence, since in the overlap every write lands in both.
     */
    private suspend fun settle(
        type: Type,
        ids: Set<String>,
        src: Int,
        dst: Int,
        repair: Boolean,
    ): Diff {
        // Bounded: a copy under live writes can leave millions of changed documents, and every one
        // in flight at once was answered 429 by the engine (measured, MoveSpeedProbe).
        val gate = kotlinx.coroutines.sync.Semaphore(SETTLE_CONCURRENCY)
        val confirmed = ArrayList<Fix?>(ids.size)
        for (chunk in ids.chunked(SETTLE_CHUNK)) {
            confirmed +=
                coroutineScope {
                    chunk
                        .map { id ->
                            async {
                                gate.withPermit { settleOne(type, id, src, dst, repair) }
                            }
                        }.awaitAll()
                }
        }
        return Diff(confirmed.count { it == Fix.MISSING }.toLong(), confirmed.count { it == Fix.DIFFERING }.toLong(), confirmed.count { it == Fix.EXTRA }.toLong())
    }

    /** One id of [settle]: read from both clusters, compared, and with [repair] fixed from the source. */
    private suspend fun settleOne(
        type: Type,
        id: String,
        src: Int,
        dst: Int,
        repair: Boolean,
    ): Fix? {
        val a = read(type, id, src)
        val b = read(type, id, dst)
        return when {
            a != null && b == null -> Fix.MISSING.also { if (repair) put(type, a, dst, ifAbsent = true) }
            a != null && type.compareFields && canonical(a["fields"]) != canonical(b!!["fields"]) -> Fix.DIFFERING.also { if (repair) put(type, a, dst, ifAbsent = false) }
            a == null && b != null -> Fix.EXTRA.also { if (repair) remove(type, id, dst) }
            else -> null
        }
    }

    /** Whether the last [diff] found nothing: what [finish] requires. */
    @Volatile private var lastDiffClean = false

    /** Set when [finish] starts removing from the sources; [diff] refuses from then on. */
    @Volatile private var finishing = false

    private enum class Fix { MISSING, DIFFERING, EXTRA }

    /**
     * Step 5, once a [diff] came back clean and every store reads by [to]: the moved documents
     * removed from their sources while the overlap package still sends them every write, then
     * [deployFinal] (the caller deploys the final package and waits for it to converge), then
     * the documents written to the sources in between removed too. What the two removals took.
     */
    suspend fun finish(deployFinal: suspend () -> Unit): Counts {
        check(lastDiffClean) { "finish() needs a clean diff() first: the destination must hold every moved document before the sources lose theirs" }
        finishing = true
        val before = cleanup()
        deployFinal()
        val after = cleanup()
        return Counts(before.events + after.events, before.reputations + after.reputations)
    }

    /**
     * The moved documents removed from their sources by a document-API `DELETE` by selection in
     * each source cluster, by the content nodes, with nothing passing through this client (the
     * first cut removed them one by one from here, at ~3,100 documents/s). Garbage collection
     * may remove some first once the sources are narrowed; the count is what this call removed.
     */
    private suspend fun cleanup(): Counts = counts { type, buckets, src, _ -> writeVisit(type, src, buckets, "", delete = true) }

    /**
     * A visit of [buckets]' documents of [type] in [cluster] that WRITES inside Vespa ([VespaHttp.writeVisit]):
     * [SLICES] slices at once, each followed to its end. The documents it touched, as the
     * document API counts them.
     */
    private suspend fun writeVisit(
        type: Type,
        cluster: Int,
        buckets: List<Int>,
        extra: String,
        delete: Boolean,
    ): Long {
        val base = visitBase(type, cluster, buckets) + extra
        val n = AtomicLong()
        coroutineScope {
            (0 until SLICES)
                .map { slice ->
                    async {
                        val sliced = "$base&sliceId=$slice"
                        var continuation: String? = null
                        while (true) {
                            val resp = http.writeVisit(continuation?.let { "$sliced&continuation=$it" } ?: sliced, delete)
                            require(resp.statusCode() < 400) {
                                "move ${if (delete) "delete" else "copy"} of ${type.doctype} in ${AuthorShards.clusterName(cluster)}: ${resp.statusCode()} ${resp.body().take(300)}"
                            }
                            val env = Json.parseToJsonElement(resp.body()).jsonObject
                            env["documentCount"]
                                ?.jsonPrimitive
                                ?.content
                                ?.toLongOrNull()
                                ?.let { n.addAndGet(it) }
                            continuation = env["continuation"]?.jsonPrimitive?.content ?: break
                        }
                    }
                }.awaitAll()
        }
        return n.get()
    }

    /** The document-API visit url over [buckets]' documents of [type] in [cluster], sliced, before its `sliceId`. */
    private fun visitBase(
        type: Type,
        cluster: Int,
        buckets: List<Int>,
    ): String =
        "${api.next()}/document/v1/${type.namespace}/${type.doctype}/docid" +
            "?cluster=${URLEncoder.encode(AuthorShards.clusterName(cluster), "UTF-8")}" +
            "&selection=${URLEncoder.encode(to.bucketTest(type.keyField, buckets), "UTF-8")}" +
            "&timeout=$SERVER_TIMEOUT_SECONDS&slices=$SLICES"

    /** What [copy] moved per (source, destination, type): [diff] sizes its batches from it. */
    private val seen = ConcurrentHashMap<Triple<Int, Int, String>, Long>()

    /** The moving buckets' documents still in their sources: zero after [finish]. */
    suspend fun remaining(): Counts =
        counts { type, buckets, src, _ ->
            val n = AtomicLong()
            visit(type, src, buckets, "[id]") { docs -> n.addAndGet(docs.size.toLong()) }
            n.get()
        }

    /** (source, destination) -> the buckets moving between them. */
    private val pairs: Map<Pair<Int, Int>, List<Int>> = moves.entries.groupBy({ it.value }, { it.key }).mapValues { (_, b) -> b.sorted() }

    /**
     * [per] over every pair's buckets, ALL of them in one visit per type: a visit evaluates its
     * selection over the whole source cluster, so a visit per batch re-read the cluster once per
     * batch (measured: copying 44 k events took 87 s, ~500 documents/s, in batches of 16). The
     * streaming steps hold nothing per document, so one visit costs no memory. [diff], which holds
     * a batch's destination in memory, still goes [BATCH] buckets at a time.
     */
    private suspend fun counts(per: suspend (Type, List<Int>, Int, Int) -> Long): Counts {
        var events = 0L
        var reputations = 0L
        for ((pair, all) in pairs) {
            events += per(types[0], all, pair.first, pair.second)
            reputations += per(types[1], all, pair.first, pair.second)
        }
        return Counts(events, reputations)
    }

    /**
     * [buckets]' documents of [type] in [cluster], page by page, projected to [fieldSet], in
     * [SLICES] slices walked at once (document-API `slices`/`sliceId`: disjoint, together the
     * whole). [onPage] is called from several coroutines at once and must be safe for that.
     */
    private suspend fun visit(
        type: Type,
        cluster: Int,
        buckets: List<Int>,
        fieldSet: String,
        extra: String = "",
        onPage: suspend (List<JsonObject>) -> Unit,
    ) {
        val base = visitBase(type, cluster, buckets) + "&fieldSet=${URLEncoder.encode(fieldSet, "UTF-8")}&wantedDocumentCount=$PAGE$extra"
        coroutineScope {
            (0 until SLICES)
                .map { slice ->
                    async {
                        val sliced = "$base&sliceId=$slice"
                        var continuation: String? = null
                        while (true) {
                            val resp = http.getVisit(continuation?.let { "$sliced&continuation=$it" } ?: sliced)
                            require(resp.statusCode() < 400) { "move visit of ${type.doctype} in ${AuthorShards.clusterName(cluster)}: ${resp.statusCode()} ${resp.body().take(300)}" }
                            val env = Json.parseToJsonElement(resp.body()).jsonObject
                            val docs = env["documents"]?.jsonArray.orEmpty().map { it.jsonObject }
                            if (docs.isNotEmpty()) onPage(docs)
                            continuation = env["continuation"]?.jsonPrimitive?.content ?: break
                        }
                    }
                }.awaitAll()
        }
    }

    /**
     * [id] as [cluster] holds it now, or null when it holds none. An id that cannot be a URL path
     * segment ([docIdSegment]) reads as present everywhere, so it is never counted as a difference
     * the point reads could not confirm.
     */
    private suspend fun read(
        type: Type,
        id: String,
        cluster: Int,
    ): JsonObject? {
        val segment = docIdSegment(id) ?: return JsonObject(mapOf("id" to JsonPrimitive("id:${type.namespace}:${type.doctype}::$id")))
        val resp = http.get("${api.next()}/document/v1/${type.namespace}/${type.doctype}/docid/$segment?cluster=${URLEncoder.encode(AuthorShards.clusterName(cluster), "UTF-8")}")
        if (resp.statusCode() == 404) return null
        require(resp.statusCode() < 400) { "move read of $id in ${AuthorShards.clusterName(cluster)}: ${resp.statusCode()}" }
        return Json.parseToJsonElement(resp.body()).jsonObject.takeIf { "fields" in it }
    }

    /** Put [doc] into [cluster] alone; [ifAbsent] only where no version exists there. True when it wrote. */
    private suspend fun put(
        type: Type,
        doc: JsonObject,
        cluster: Int,
        ifAbsent: Boolean,
    ): Boolean {
        val fields = (doc["fields"] as? JsonObject)?.let(::writable) ?: return false
        var params = OperationParameters.empty().route(AuthorShards.clusterName(cluster)).timeout(TIMEOUT)
        if (ifAbsent) params = params.testAndSetCondition("false").createIfNonExistent(true)
        val result =
            feed.client
                .put(DocumentId.of(type.namespace, type.doctype, idOf(doc)), "{\"fields\":$fields}", params)
                .await()
        return result.type() == Result.Type.success
    }

    /** Remove [id] from [cluster] alone. True when the engine acknowledged it. */
    private suspend fun remove(
        type: Type,
        id: String,
        cluster: Int,
    ): Boolean =
        feed.client
            .remove(DocumentId.of(type.namespace, type.doctype, id), OperationParameters.empty().route(AuthorShards.clusterName(cluster)).timeout(TIMEOUT))
            .await()
            .type() == Result.Type.success

    override fun close() {
        try {
            feed.close()
        } finally {
            http.close()
        }
    }

    private companion object {
        const val REPUTATION = "reputation"
        const val PAGE = 1024

        /** Buckets per visit: each visit evaluates its selection over the whole source cluster. */
        const val BATCH = 16

        /** Documents one diff batch may hold in memory (ids, plus whole documents for reputation): ~200 MB of ids. */
        const val DIFF_BUDGET = 2_000_000.0

        /** How far the content nodes' clocks may run behind this client's ([changedSince]): generous, as it only widens a pass. */
        const val CLOCK_MARGIN_MS = 10 * 60_000L

        /** Point reads [settle] keeps in flight. */
        const val SETTLE_CONCURRENCY = 64

        /** Ids [settle] starts coroutines for at a time, so millions of candidates are not millions of coroutines. */
        const val SETTLE_CHUNK = 10_000

        /** A 64-bit hash of a document id (FNV-1a), for the per-bucket id-set digests. */
        fun idHash(id: String): Long {
            var h = -0x340d631b7bdddcdbL
            for (c in id) {
                h = h xor c.code.toLong()
                h *= 0x100000001b3L
            }
            return h
        }

        /** Concurrent slices of one visit: within the ~64-session ceiling that wedged a small node's document API (VespaVisits). */
        const val SLICES = 8
        const val SERVER_TIMEOUT_SECONDS = 90L
        val TIMEOUT: Duration = Duration.ofSeconds(30)

        /** The document id's specific part: what follows `id:<namespace>:<type>::`. */
        fun idOf(doc: JsonObject): String = doc["id"]!!.jsonPrimitive.content.substringAfter("::")

        /**
         * [fields] as a put takes them: a visit writes a tensor as `{"type": …, "cells": …}`, and the
         * type belongs to the schema, not the document.
         */
        fun writable(fields: JsonObject): String =
            JsonObject(
                fields.mapValues { (_, v) ->
                    if (v is JsonObject && "type" in v && ("cells" in v || "blocks" in v || "values" in v)) JsonObject(v - "type") else v
                },
            ).toString()

        /** [e] with object keys sorted at every depth, so two equal documents compare equal as strings. */
        fun canonical(e: JsonElement?): String =
            when (e) {
                null -> ""
                is JsonObject -> e.entries.sortedBy { it.key }.joinToString(",", "{", "}") { (k, v) -> "\"$k\":${canonical(v)}" }
                is JsonArray -> e.joinToString(",", "[", "]") { canonical(it) }
                is JsonPrimitive -> e.toString()
            }
    }
}
