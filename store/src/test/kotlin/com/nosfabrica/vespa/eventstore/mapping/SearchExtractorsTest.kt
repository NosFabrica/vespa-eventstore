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
package com.nosfabrica.vespa.eventstore.mapping

import com.nosfabrica.vespa.eventstore.engine.doc.SearchFields
import com.vitorpamplona.quartz.experimental.nip82SoftwareApps.application.SoftwareApplicationEvent
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip17Dm.messages.ChatMessageEvent
import com.vitorpamplona.quartz.nip23LongContent.LongTextNoteEvent
import com.vitorpamplona.quartz.nip34Git.repository.GitRepositoryEvent
import com.vitorpamplona.quartz.nip35Torrents.TorrentEvent
import com.vitorpamplona.quartz.nip89AppHandlers.definition.AppDefinitionEvent
import com.vitorpamplona.quartz.nip99Classifieds.ClassifiedsEvent
import com.vitorpamplona.quartz.nipB0WebBookmarks.WebBookmarkEvent
import com.vitorpamplona.quartz.nipC0CodeSnippets.CodeSnippetEvent
import kotlin.test.Test
import kotlin.test.assertEquals

class SearchExtractorsTest {
    private val alice = "a1".repeat(32)

    @Test
    fun `kind 0 decomposes into the Brainstorm profile group`() {
        val content = """{"name":"vitor","display_name":"Vitor P","about":"builds nostr","nip05":"vitor@vitorpamplona.com","lud16":"me@wallet.com","website":"https://vitorpamplona.com","picture":"https://x/y.jpg"}"""
        val fields = SearchExtractors.extract(MetadataEvent("1".repeat(64), alice, 1L, emptyArray(), content, ""))
        assertEquals(
            SearchFields(
                name = "vitor",
                displayName = "Vitor P",
                about = "builds nostr",
                nip05 = "vitor@vitorpamplona.com",
                lud16 = "me@wallet.com",
                website = "https://vitorpamplona.com",
            ),
            fields,
        )
    }

    @Test
    fun `long-form decomposes into title, summary plus hashtags, content`() {
        val tags = arrayOf(arrayOf("d", "post"), arrayOf("title", "My Post"), arrayOf("summary", "tl;dr"), arrayOf("t", "nostr"), arrayOf("t", "search"))
        val fields = SearchExtractors.extract(LongTextNoteEvent("2".repeat(64), alice, 1L, tags, "the whole article", ""))
        assertEquals(SearchFields(primary = "My Post", secondary = "tl;dr\nnostr search", text = "the whole article"), fields)
    }

    @Test
    fun `notes use the NIP-14 subject and hashtags`() {
        val tags = arrayOf(arrayOf("subject", "meetup"), arrayOf("t", "brazil"))
        val fields = SearchExtractors.extract(TextNoteEvent("3".repeat(64), alice, 1L, tags, "see you there", ""))
        assertEquals(SearchFields(primary = "meetup", secondary = "brazil", text = "see you there"), fields)
    }

    @Test
    fun `unmapped searchable kinds fall back to indexableContent in the tertiary tier`() {
        val fields = SearchExtractors.extract(ChatMessageEvent("4".repeat(64), alice, 1L, emptyArray(), "hello group", ""))
        assertEquals(SearchFields(text = "hello group"), fields)
    }

    @Test
    fun `app handler metadata reuses the kind-0 profile columns`() {
        val content = """{"name":"Damus","display_name":"Damus App","about":"a nostr client","nip05":"_@damus.io","lud16":"tips@damus.io","website":"https://damus.io"}"""
        val fields = SearchExtractors.extract(AppDefinitionEvent("6".repeat(64), alice, 1L, emptyArray(), content, ""))
        assertEquals(
            SearchFields(
                name = "Damus",
                displayName = "Damus App",
                about = "a nostr client",
                nip05 = "_@damus.io",
                lud16 = "tips@damus.io",
                website = "https://damus.io",
            ),
            fields,
        )
    }

    @Test
    fun `git repositories route their web url to the affiliation website column`() {
        val tags = arrayOf(arrayOf("d", "repo"), arrayOf("name", "cool-repo"), arrayOf("description", "a git tool"), arrayOf("web", "https://cool.dev"))
        val fields = SearchExtractors.extract(GitRepositoryEvent("7".repeat(64), alice, 1L, tags, "", ""))
        assertEquals(SearchFields(primary = "cool-repo", secondary = "a git tool", website = "https://cool.dev"), fields)
    }

    @Test
    fun `web bookmarks route the bookmarked url to the website column`() {
        val tags = arrayOf(arrayOf("d", "example.com/article"), arrayOf("title", "Great Article"))
        val fields = SearchExtractors.extract(WebBookmarkEvent("8".repeat(64), alice, 1L, tags, "a description", ""))
        assertEquals(SearchFields(primary = "Great Article", secondary = "a description", website = "https://example.com/article"), fields)
    }

    @Test
    fun `software apps route homepage and repository urls to the website column`() {
        val tags = arrayOf(arrayOf("d", "com.example.app"), arrayOf("name", "Example App"), arrayOf("summary", "does things"), arrayOf("url", "https://example.com"), arrayOf("repository", "https://github.com/ex/app"))
        val fields = SearchExtractors.extract(SoftwareApplicationEvent("9".repeat(64), alice, 1L, tags, "", ""))
        assertEquals(SearchFields(primary = "Example App", secondary = "does things", website = "https://example.com\nhttps://github.com/ex/app"), fields)
    }

    @Test
    fun `hashtags and location are folded in systemically for every kind`() {
        val tags = arrayOf(arrayOf("title", "Sofa"), arrayOf("summary", "comfy"), arrayOf("location", "Berlin, DE"), arrayOf("t", "furniture"))
        val fields = SearchExtractors.extract(ClassifiedsEvent("a".repeat(64), alice, 1L, tags, "a used sofa", ""))
        assertEquals(
            SearchFields(primary = "Sofa", secondary = "comfy\nfurniture", text = "a used sofa", location = "Berlin, DE"),
            fields,
        )
    }

    @Test
    fun `torrents index file names in the secondary tier and trackers as website`() {
        val tags = arrayOf(arrayOf("title", "Ubuntu ISO"), arrayOf("file", "ubuntu-24.04.iso"), arrayOf("file", "readme.txt"), arrayOf("tracker", "udp://tracker.example.com:80"))
        val fields = SearchExtractors.extract(TorrentEvent("b".repeat(64), alice, 1L, tags, "linux distro", ""))
        assertEquals(
            SearchFields(primary = "Ubuntu ISO", secondary = "ubuntu-24.04.iso\nreadme.txt", text = "linux distro", website = "udp://tracker.example.com:80"),
            fields,
        )
    }

    @Test
    fun `code snippets index language and runtime keywords plus the repo url`() {
        val tags = arrayOf(arrayOf("name", "hello.py"), arrayOf("description", "prints hello"), arrayOf("l", "python"), arrayOf("extension", "py"), arrayOf("runtime", "python 3.11"), arrayOf("repo", "https://github.com/x/y"))
        val fields = SearchExtractors.extract(CodeSnippetEvent("c".repeat(64), alice, 1L, tags, "print('hello')", ""))
        assertEquals(
            SearchFields(primary = "hello.py", secondary = "prints hello\npython\npy\npython 3.11", text = "print('hello')", website = "https://github.com/x/y"),
            fields,
        )
    }

    @Test
    fun `non-searchable kinds stay invisible`() {
        assertEquals(SearchFields.NONE, SearchExtractors.extract(Event("5".repeat(64), alice, 1L, 7, emptyArray(), "+", "")))
    }
}
