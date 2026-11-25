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
package com.nosfabrica.vespa.eventstore.engine.app

import org.w3c.dom.Element
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import java.util.zip.ZipOutputStream
import javax.xml.XMLConstants
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.transform.TransformerFactory
import javax.xml.transform.dom.DOMSource
import javax.xml.transform.stream.StreamResult

/**
 * The Vespa application package (event + reputation schemas, rank profiles),
 * bundled into this jar as a zip by the build — shipped with the code so
 * schema and code can never drift.
 *
 * The package is deployed VERBATIM apart from one deployment-shaped choice:
 * whether the container writes an access log. That cannot be a compile-time
 * constant because it depends on who is talking to the engine. A store fed by
 * its own mirroring router logs mostly itself — the write path is read-heavy,
 * so ~500 req/s of dedup existence checks become ~3.2 GB/hour of JSON nobody
 * reads — while a store answering real client queries wants exactly that
 * record. Hence [ACCESS_LOG_ENV] rather than a fork of services.xml.
 *
 * And one PROTOTYPE topology choice: the author-shard count ([zipBytes]'s `authorShards`) splits the content cluster into several,
 * by author, each holding its authors' events and reputation documents ([shardContent]).
 */
object VespaApp {
    /** Classpath location of the bundled package (see vespa/build.gradle.kts). */
    const val RESOURCE = "/vespa-app.zip"

    /** Env var controlling the container's access log. Unset keeps Vespa's default. */
    const val ACCESS_LOG_ENV = "VESPA_ACCESS_LOG"

    /**
     * Accepted values. Deliberately an OFF SWITCH rather than a passthrough of
     * every `<accesslog type=…>` Vespa has: configuring the log at all obliges
     * the package to restate `fileNamePattern` (Vespa fails the deploy without
     * it), so anything else means owning defaults better left inherited. A
     * deployment that wants to shape the log should say so in services.xml.
     *
     * "json" is NOT accepted even though it is what Vespa writes today: it would
     * be a no-op coinciding with an upstream default, so the day that default
     * moves the variable becomes a lie the operator cannot detect.
     */
    val ACCESS_LOG_VALUES = setOf("default", "disabled")

    private const val SERVICES = "services.xml"
    private const val CONTAINER = "container"
    private const val ACCESSLOG = "accesslog"
    private const val CONTENT = "content"
    private const val DOCUMENT = "document"

    /**
     * The zipped application package bytes, ready to POST to a Vespa config
     * server. Honors [ACCESS_LOG_ENV]; unset ships the package as built.
     */
    fun zipBytes(): ByteArray = zipBytes(System.getenv(ACCESS_LOG_ENV))

    /**
     * An author-shard count parsed STRICTLY: unset or blank is one shard, the package as built,
     * and anything but a whole number in 1..[AuthorShards.MAX] is refused rather than clamped,
     * since the count decides where documents LIVE. Not read from the environment here: the
     * setting reaches the package only as a value (`EngineTuning.authorShards`, whose `fromEnv`
     * reads [AuthorShards.ENV]), so the deployed package and the client cannot read it apart.
     */
    fun authorShardsOf(raw: String?): Int {
        val value = raw?.trim()?.takeIf { it.isNotEmpty() } ?: return 1
        return value.toIntOrNull()?.takeIf { it in 1..AuthorShards.MAX }
            ?: throw IllegalArgumentException("${AuthorShards.ENV}='$raw' is not a whole number in 1..${AuthorShards.MAX}")
    }

    /**
     * [zipBytes] with the access-log choice passed in rather than read from the
     * environment — the programmatic form of [ACCESS_LOG_ENV], for an embedder
     * that configures the store in code and for the integration test that has
     * to DEPLOY the rewritten package (an env var cannot be set from inside the
     * JVM that reads it). `null` or blank means "as built".
     */
    fun zipBytes(
        accessLog: String?,
        authorShards: Int = 1,
    ): ByteArray {
        require(authorShards in 1..AuthorShards.MAX) { "authorShards $authorShards is outside 1..${AuthorShards.MAX}" }
        return zipBytes(accessLog, AuthorShards.BucketMap.even(authorShards))
    }

    /** [zipBytes] with the authors split by an explicit [buckets] map ([AuthorShards.BucketMap]); a one-cluster map ships the package as built. */
    fun zipBytes(
        accessLog: String?,
        buckets: AuthorShards.BucketMap,
    ): ByteArray {
        val raw =
            VespaApp::class.java.getResourceAsStream(RESOURCE)?.use { it.readBytes() }
                ?: error("bundled Vespa application package not found on the classpath ($RESOURCE) - is :engine on the runtime classpath?")

        val value = accessLog?.trim()?.lowercase()?.takeIf { it.isNotEmpty() }
        value?.let {
            require(it in ACCESS_LOG_VALUES) {
                "$ACCESS_LOG_ENV='$accessLog' is not one of ${ACCESS_LOG_VALUES.sorted().joinToString(", ")}"
            }
        }
        val disable = value == "disabled"
        if (!disable && buckets.clusters == 1) return raw

        return rewrite(raw, SERVICES) { xml ->
            val sharded = if (buckets.clusters > 1) shardContent(xml, buckets) else xml
            if (disable) disableAccessLog(sharded) else sharded
        }
    }

    /**
     * PROTOTYPE: [servicesXml] with its one content cluster split into [shards] ([AuthorShards]).
     * Shard 0 is the original element, keeping its id; shards 1.. are copies of it — same nodes,
     * same tuning — with ids [AuthorShards.clusterName], each document type ANDed with the shard's
     * selection: `event` on its `pubkey` field (its id is the event's), `reputation` on its id
     * (the subject; a cell update carries no `pubkey` to route by).
     *
     * The copies share the original's `<nodes>`, which on the bundled single-host package is
     * one host running one search node per cluster. A multi-host deployment that writes its own
     * services.xml (`docs/services-multinode-example.xml`) splits it with [shardServices], which
     * also gives each cluster its own nodes.
     *
     * NOT A MIGRATION. With `garbage-collection="true"` a cluster's selection is also what its
     * content nodes KEEP: deploying a new shard count over a corpus fed under another one makes
     * the engine delete every document the new selections move away, and nothing re-feeds them.
     */
    internal fun shardContent(
        servicesXml: String,
        shards: Int,
    ): String {
        require(shards in 2..AuthorShards.MAX) { "shards $shards is outside 2..${AuthorShards.MAX}" }
        return shardContent(servicesXml, AuthorShards.BucketMap.even(shards))
    }

    /**
     * [shardContent] by an explicit [buckets] map. The map's spec is also written into the file as
     * a comment ([BUCKETS_MARK]), so the deploy guard reads back what each ACTIVE cluster keeps
     * (`SchemaDeployer.checkShardChange`) without parsing the selections.
     */
    internal fun shardContent(
        servicesXml: String,
        buckets: AuthorShards.BucketMap,
    ): String {
        val shards = buckets.clusters
        require(shards >= 2) { "a one-cluster map needs no split" }
        val doc = parse(servicesXml)
        val contents = doc.getElementsByTagName(CONTENT)
        require(contents.length == 1) { "$SERVICES declares ${contents.length} <$CONTENT> elements; author sharding splits exactly one" }
        val base = contents.item(0) as Element
        require(base.getAttribute("id") == AuthorShards.BASE_CLUSTER) { "$SERVICES' content cluster is '${base.getAttribute("id")}', not '${AuthorShards.BASE_CLUSTER}'" }

        val copies = (1 until shards).map { (base.cloneNode(true) as Element).apply { setAttribute("id", AuthorShards.clusterName(it)) } }
        (listOf(base) + copies).forEachIndexed { index, cluster ->
            val types = cluster.getElementsByTagName(DOCUMENT)
            val seen = HashSet<String>()
            for (i in 0 until types.length) {
                val type = types.item(i) as Element
                val name = type.getAttribute("type")
                val shard =
                    when (name) {
                        "event" -> buckets.selection("event.pubkey", index)
                        "reputation" -> buckets.selection("id.specific", index)
                        else -> error("$SERVICES' content cluster holds document type '$name', which author sharding has no key for")
                    }
                seen += name
                val existing = type.getAttribute("selection")
                type.setAttribute("selection", if (existing.isBlank()) shard else "($existing) and $shard")
            }
            require(seen == setOf("event", "reputation")) { "$SERVICES' content cluster holds $seen; author sharding needs event and reputation together" }
        }
        doc.documentElement.insertBefore(doc.createComment(" $BUCKETS_MARK${buckets.spec()} "), doc.documentElement.firstChild)
        doc.documentElement.insertBefore(doc.createTextNode("\n  "), doc.documentElement.firstChild)
        var anchor: org.w3c.dom.Node = base
        for (copy in copies) {
            val gap = doc.createTextNode("\n\n  ")
            base.parentNode.insertBefore(gap, anchor.nextSibling)
            base.parentNode.insertBefore(copy, gap.nextSibling)
            anchor = copy
        }
        return serialize(doc, servicesXml.length * shards)
    }

    /**
     * An OPERATOR'S OWN multi-host [servicesXml] split into the store's author-shard clusters by
     * [buckets], for a deployment that deploys its own package (`AUTO_DEPLOY=false` in
     * vespa-relay) rather than the bundled single-host one (`docs/author-shards.md`, "A
     * multi-node package"). The package must have exactly one content cluster, `content`, holding
     * `event` and `reputation`, with its nodes listed flat (no `<group>`).
     *
     * [nodeGroups] names, by `distribution-key`, the nodes each cluster gets: cluster i keeps only
     * the nodes in `nodeGroups[i]`, so each node holds 1/K of the reputation documents, which is
     * the point. Every node must be in exactly one group, and each group must hold at least the
     * cluster's `<redundancy>` (and `<min-redundancy>`) nodes, or Vespa could not place every copy.
     * Null keeps every node in every cluster, which is only right for one host (the bundled package:
     * one search node per cluster on it), and is refused for a package listing more than one node.
     *
     * Everything else in the cluster (tuning, `<config>`, `<engine>`, expiry selection) is copied to
     * every cluster as written, and the bucket map is recorded as the comment the deploy guard
     * reads ([bucketMapOf]), so `SchemaDeployer.checkShardChange` judges this package as it judges
     * the bundled one. A one-cluster map returns [servicesXml] unchanged.
     */
    fun shardServices(
        servicesXml: String,
        buckets: AuthorShards.BucketMap,
        nodeGroups: List<Set<Int>>?,
    ): String {
        if (buckets.clusters == 1) {
            require(nodeGroups == null || nodeGroups.size == 1) { "a one-cluster map takes one node group, not ${nodeGroups?.size}" }
            return servicesXml
        }
        val source = parse(servicesXml)
        val contents = source.getElementsByTagName(CONTENT)
        require(contents.length == 1) { "$SERVICES declares ${contents.length} <$CONTENT> elements; author sharding splits exactly one" }
        val base = contents.item(0) as Element
        require(base.getElementsByTagName("group").length == 0) {
            "$SERVICES' content cluster uses <group> (grouped distribution); split it by hand: shardServices handles a flat <nodes> list"
        }
        val keys = nodeKeys(base)
        if (nodeGroups == null) {
            require(keys.size <= 1) {
                "$SERVICES' content cluster lists ${keys.size} nodes: name which go to each of the ${buckets.clusters} clusters " +
                    "(otherwise every node would hold every cluster, and none would hold fewer reputation documents)"
            }
            return shardContent(servicesXml, buckets)
        }
        require(nodeGroups.size == buckets.clusters) { "${nodeGroups.size} node groups for ${buckets.clusters} clusters" }
        nodeGroups.forEachIndexed { i, g -> require(g.isNotEmpty()) { "cluster ${AuthorShards.clusterName(i)} has no nodes" } }
        val all = nodeGroups.flatten()
        require(all.size == all.toSet().size) { "a node is in two groups: ${all.groupingBy { it }.eachCount().filterValues { it > 1 }.keys.sorted()}" }
        require(all.toSet() == keys.toSet()) {
            "the groups name distribution keys ${all.toSet().sorted()}, the content cluster lists ${keys.sorted()}: every node goes to exactly one cluster"
        }
        val copies = listOf("redundancy", "min-redundancy").mapNotNull { tag -> firstText(base, tag)?.toIntOrNull()?.let { tag to it } }
        for ((tag, n) in copies) {
            nodeGroups.forEachIndexed { i, g ->
                require(g.size >= n) { "cluster ${AuthorShards.clusterName(i)} gets ${g.size} node(s), fewer than its <$tag> of $n" }
            }
        }
        // Split as for one host, then drop from each cluster the nodes it does not get.
        val doc = parse(shardContent(servicesXml, buckets))
        val clusters = doc.getElementsByTagName(CONTENT)
        for (i in 0 until clusters.length) {
            val cluster = clusters.item(i) as Element
            val index = AuthorShards.clusterIndex(cluster.getAttribute("id")) ?: error("unexpected content cluster '${cluster.getAttribute("id")}'")
            val nodes = directNodes(cluster)
            for (node in nodes) {
                val key = node.getAttribute("distribution-key").toInt()
                if (key !in nodeGroups[index]) {
                    // The node and the whitespace before it, so the list keeps its indentation.
                    (node.previousSibling?.takeIf { it.nodeType == org.w3c.dom.Node.TEXT_NODE && it.nodeValue.isBlank() })?.let { node.parentNode.removeChild(it) }
                    node.parentNode.removeChild(node)
                }
            }
        }
        return serialize(doc, servicesXml.length * buckets.clusters)
    }

    /**
     * Node groups written `0,1,2;3,4`: cluster i gets the distribution keys of the i-th
     * `;`-separated list. What `shardPackage` reads from `SHARD_NODES`.
     */
    fun parseNodeGroups(spec: String): List<Set<Int>> =
        spec.split(';').map { group ->
            group
                .split(',')
                .map { it.trim() }
                .filter { it.isNotEmpty() }
                .map { it.toIntOrNull() ?: throw IllegalArgumentException("'$it' in '$spec' is not a distribution key") }
                .toSet()
        }

    /** The distribution keys of [content]'s `<nodes>` children, in order; a node without one is refused. */
    private fun nodeKeys(content: Element): List<Int> =
        directNodes(content).map { n ->
            n.getAttribute("distribution-key").toIntOrNull() ?: error("a content node has no distribution-key: ${n.getAttribute("hostalias")}")
        }

    /** The `<node>` elements directly under [content]'s `<nodes>`. */
    private fun directNodes(content: Element): List<Element> {
        val lists = content.getElementsByTagName("nodes")
        val out = ArrayList<Element>()
        for (i in 0 until lists.length) {
            val list = lists.item(i) as Element
            if (list.parentNode != content) continue
            val children = list.childNodes
            for (j in 0 until children.length) (children.item(j) as? Element)?.takeIf { it.tagName == "node" }?.let(out::add)
        }
        return out
    }

    private fun firstText(
        parent: Element,
        tag: String,
    ): String? =
        parent
            .getElementsByTagName(tag)
            .item(0)
            ?.textContent
            ?.trim()

    /**
     * Return [servicesXml] with `<accesslog type="disabled" />` as the
     * container's first child (`<nodes>` stays last, as Vespa's examples have
     * it). Parsed rather than string-spliced: services.xml documents this knob
     * in prose, so a textual search for the tag name reads its own comment and
     * refuses a perfectly good package. The parser checks the structure that
     * actually matters — this container has no accesslog CHILD.
     *
     * The serializer reformats: comments and indentation survive, but the XML
     * declaration is rewritten and attribute quoting normalized, so the deployed
     * services.xml is not byte-equal to `engine/app/services.xml`. Only Vespa
     * reads it, and the unset path ships the package untouched.
     */
    internal fun disableAccessLog(servicesXml: String): String {
        val doc = parse(servicesXml)

        // A silent no-op here would be the worst outcome: the operator sets the
        // variable, the deploy succeeds, and the log keeps filling the disk.
        // Fail instead if the element this hangs off ever moves.
        val containers = doc.getElementsByTagName(CONTAINER)
        val container =
            when (containers.length) {
                1 -> containers.item(0) as Element

                0 -> error("$SERVICES has no <$CONTAINER> element to configure — $ACCESS_LOG_ENV would be silently ignored")

                // Which one to disable is a real choice, not a default to guess.
                else -> error("$SERVICES declares ${containers.length} <$CONTAINER> elements — $ACCESS_LOG_ENV cannot pick one")
            }
        require(container.getElementsByTagName(ACCESSLOG).length == 0) {
            "$SERVICES already declares an access log; configure it there rather than via $ACCESS_LOG_ENV"
        }

        // Indentation is written by hand rather than by the serializer: it only
        // has to make the injected pair read like the file around it, and
        // OutputKeys.INDENT would reflow every other element to get there.
        val first = container.firstChild
        container.insertBefore(doc.createTextNode("\n    "), first)
        container.insertBefore(doc.createComment(" injected by VespaApp: $ACCESS_LOG_ENV=disabled "), first)
        container.insertBefore(doc.createTextNode("\n    "), first)
        container.insertBefore(doc.createElement(ACCESSLOG).apply { setAttribute("type", "disabled") }, first)

        return serialize(doc, servicesXml.length + 128)
    }

    /** The comment [shardContent] writes the bucket map under. */
    internal const val BUCKETS_MARK = "eventstore author buckets: "

    /** The bucket map [shardContent] wrote into [servicesXml]; null when it carries none (one cluster, or a package of someone else's). */
    internal fun bucketMapOf(servicesXml: String): AuthorShards.BucketMap? {
        val doc = parse(servicesXml)
        val nodes = doc.documentElement.childNodes
        for (i in 0 until nodes.length) {
            val n = nodes.item(i)
            if (n.nodeType == org.w3c.dom.Node.COMMENT_NODE && n.nodeValue.trim().startsWith(BUCKETS_MARK)) {
                return AuthorShards.BucketMap.parse(n.nodeValue.trim().removePrefix(BUCKETS_MARK))
            }
        }
        return null
    }

    /** The content cluster ids [servicesXml] declares, in order. */
    internal fun contentClusters(servicesXml: String): List<String> {
        val contents = parse(servicesXml).getElementsByTagName(CONTENT)
        return (0 until contents.length).map { (contents.item(it) as Element).getAttribute("id") }
    }

    private fun parse(servicesXml: String): org.w3c.dom.Document =
        DocumentBuilderFactory
            .newInstance()
            .apply {
                // Our own file, but the parser is hardened anyway: nothing
                // in an application package needs a doctype or an entity.
                setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true)
                setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
                isXIncludeAware = false
                isExpandEntityReferences = false
            }.newDocumentBuilder()
            .parse(ByteArrayInputStream(servicesXml.encodeToByteArray()))

    private fun serialize(
        doc: org.w3c.dom.Document,
        sizeHint: Int,
    ): String {
        val out = ByteArrayOutputStream(sizeHint)
        TransformerFactory
            .newInstance()
            .apply { setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true) }
            .newTransformer()
            .transform(DOMSource(doc), StreamResult(out))
        return out.toByteArray().decodeToString()
    }

    /**
     * Copy [zip] entry by entry, passing only [target] through [transform].
     * Every other entry is copied as RAW BYTES — decoding them as text would
     * silently replace anything that is not valid UTF-8 with U+FFFD, and the
     * package carries schemas, not just this one file.
     */
    private fun rewrite(
        zip: ByteArray,
        target: String,
        transform: (body: String) -> String,
    ): ByteArray {
        val out = ByteArrayOutputStream(zip.size)
        ZipOutputStream(out).use { zos ->
            ZipInputStream(ByteArrayInputStream(zip)).use { zis ->
                var entry = zis.nextEntry
                while (entry != null) {
                    val bytes = zis.readBytes()
                    val body = if (entry.name == target) transform(bytes.decodeToString()).encodeToByteArray() else bytes
                    // A fresh entry, so the sizes and CRC are recomputed for
                    // the rewritten bytes rather than copied from the source.
                    zos.putNextEntry(ZipEntry(entry.name))
                    if (!entry.isDirectory) zos.write(body)
                    zos.closeEntry()
                    entry = zis.nextEntry
                }
            }
        }
        return out.toByteArray()
    }
}
