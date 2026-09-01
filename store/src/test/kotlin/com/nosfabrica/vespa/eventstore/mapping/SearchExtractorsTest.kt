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
import com.vitorpamplona.quartz.experimental.trustedLists.users.UserTrustedListEvent
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.metadata.MetadataEvent
import com.vitorpamplona.quartz.nip10Notes.TextNoteEvent
import com.vitorpamplona.quartz.nip17Dm.messages.ChatMessageEvent
import com.vitorpamplona.quartz.nip23LongContent.LongTextNoteEvent
import com.vitorpamplona.quartz.nip34Git.repository.GitRepositoryEvent
import com.vitorpamplona.quartz.nip35Torrents.TorrentEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
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
    fun `a declared shortcode is one term of its own, not a name token`() {
        // The reported shape: a bridged Mastodon profile whose display name
        // carries a custom-emoji badge, declared as one in the event's own
        // tags. Tokenized as text it put this account on the 130 000 name rung
        // for the word "verified"; as its own term it keeps the badge
        // findable, on the weak rung, as the badge it is.
        val content = """{"name":"DotardTed :verified:","about":"raw humanity :thinking: and more"}"""
        val tags = arrayOf(arrayOf("emoji", "verified", "https://static/verified.png"), arrayOf("emoji", "thinking", "https://static/think.png"))
        val fields = SearchExtractors.extract(MetadataEvent("5".repeat(64), alice, 1L, tags, content, ""))
        assertEquals("DotardTed", fields.name, "the picture leaves the name it decorated, whole and its own length")
        assertEquals("raw humanity and more", fields.about, "…in every field the event indexes")
        assertEquals("xemojiverified xemojithinking", fields.secondary, "both badges, once each, on the secondary tier")
    }

    @Test
    fun `a shortcode term is flattened to one alphanumeric token`() {
        // Vespa splits at `_` and `-` as readily as at `:`. An unflattened
        // `xemoji_official_verified` would index as three tokens, one of them
        // the bare `verified` this whole rewrite exists to remove.
        val content = """{"name":":official_verified: Em"}"""
        val tags = arrayOf(arrayOf("emoji", "official_verified", "https://static/ov.png"))
        val fields = SearchExtractors.extract(MetadataEvent("9".repeat(64), alice, 1L, tags, content, ""))
        assertEquals("Em", fields.name)
        assertEquals("xemojiofficialverified", fields.secondary)
    }

    @Test
    fun `a declared badge the text never wears indexes nothing`() {
        // Declaration is not use: an emoji tag left over from an edit names a
        // picture no field carries, and an unworn badge is not a badge.
        val content = """{"name":"DotardTed"}"""
        val tags = arrayOf(arrayOf("emoji", "verified", "https://static/verified.png"))
        val fields = SearchExtractors.extract(MetadataEvent("b".repeat(64), alice, 1L, tags, content, ""))
        assertEquals(SearchFields(name = "DotardTed"), fields)
    }

    @Test
    fun `an undeclared colon run is left exactly as it is`() {
        // `:[a-z0-9_]+:` is also what a clock looks like. Nothing here is
        // declared as an emoji, so nothing is a picture, and a regex would
        // have turned the first of these into "845".
        val content = """{"name":"8:30:45","about":"ratio 1:2:1 and :verified: said by nobody"}"""
        val fields = SearchExtractors.extract(MetadataEvent("6".repeat(64), alice, 1L, emptyArray(), content, ""))
        assertEquals("8:30:45", fields.name)
        assertEquals("ratio 1:2:1 and :verified: said by nobody", fields.about)
        assertEquals(null, fields.secondary, "nothing was declared, so nothing is a badge")
    }

    @Test
    fun `a name that is nothing but badges is still findable as its badge`() {
        // The reason this is a tokenization and not a deletion: deleting the
        // run deletes the account. The name is empty because it was a picture,
        // and the picture is what is left to find it by.
        val content = """{"name":":verified:","display_name":" :verified: "}"""
        val tags = arrayOf(arrayOf("emoji", "verified", "https://static/verified.png"))
        val fields = SearchExtractors.extract(MetadataEvent("7".repeat(64), alice, 1L, tags, content, ""))
        assertEquals(null, fields.name, "an empty name is absent, not blank")
        assertEquals(null, fields.displayName)
        assertEquals("xemojiverified", fields.secondary, "declared twice, indexed once")
    }

    @Test
    fun `a declared shortcode is rewritten in a titled kind too`() {
        val tags = arrayOf(arrayOf("d", "post"), arrayOf("title", "My :verified: Post"), arrayOf("emoji", "verified", "https://static/v.png"))
        val fields = SearchExtractors.extract(LongTextNoteEvent("8".repeat(64), alice, 1L, tags, "body", ""))
        assertEquals("My Post", fields.primary)
        assertEquals("xemojiverified", fields.secondary, "the badge tier is the same for every kind")
    }

    @Test
    fun `badge terms ride beside a kind's own secondary tier, never over it`() {
        val tags =
            arrayOf(
                arrayOf("d", "post"),
                arrayOf("title", "My Post"),
                arrayOf("summary", "tl;dr :verified:"),
                arrayOf("t", "nostr"),
                arrayOf("emoji", "verified", "https://static/v.png"),
            )
        val fields = SearchExtractors.extract(LongTextNoteEvent("c".repeat(64), alice, 1L, tags, "body", ""))
        assertEquals("tl;dr\nnostr\nxemojiverified", fields.secondary)
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
    fun `a trusted list title fills the title column, not the body`() {
        // Upstream's `is TrustedListEvent ->` branch, seen from this side: the
        // title reaches `search_primary` and NOTHING else is indexed. Before the
        // b10be95a6e pin it landed in `text`, which cost the phrase the title
        // rung and the prefix/typo columns — the `Verified Human` regression.
        val tags =
            arrayOf(
                arrayOf("d", "tl-pin-podcaster"),
                arrayOf("title", "Podcaster"),
                arrayOf("metric", "pinned-tag-membership"),
                arrayOf("p", alice, "", "87"),
            )
        val fields = SearchExtractors.extract(UserTrustedListEvent("d".repeat(64), alice, 1L, tags, """{"members":[]}""", ""))
        assertEquals(SearchFields(primary = "Podcaster"), fields)
    }

    @Test
    fun `a contact card petname fills the title column and its topics ride the secondary`() {
        // The join policy is OURS, not upstream's: quartz hands back petname,
        // summary and the topics as a separate hashtag role, and THIS schema
        // folds hashtags into the secondary column beside the summary. So a
        // provider's petname competes on the name rung — which is the whole
        // point for a people search — and its topics stay searchable text
        // rather than becoming keywords.
        val tags =
            arrayOf(
                arrayOf("d", alice),
                arrayOf("petname", "Bramblecast"),
                arrayOf("summary", "vouched by two independent raters"),
                arrayOf("t", "podcast"),
                arrayOf("rank", "90"),
            )
        val fields = SearchExtractors.extract(ContactCardEvent("e".repeat(64), alice, 1L, tags, "", ""))
        assertEquals(SearchFields(primary = "Bramblecast", secondary = "vouched by two independent raters\npodcast"), fields)
    }

    @Test
    fun `a contact card with only topics still indexes them`() {
        // The shape quartz's own ContactCardEvent.build() produces: petname and
        // summary go in the NIP-44 content, so the public card carries nothing
        // but its topics. They must survive the fold into the secondary column
        // rather than collapsing the extraction to NONE.
        val tags = arrayOf(arrayOf("d", alice), arrayOf("t", "podcast"), arrayOf("t", "bitcoin"), arrayOf("rank", "90"))
        val fields = SearchExtractors.extract(ContactCardEvent("f".repeat(64), alice, 1L, tags, "encrypted", ""))
        assertEquals(SearchFields(secondary = "podcast bitcoin"), fields)
    }

    @Test
    fun `non-searchable kinds stay invisible`() {
        assertEquals(SearchFields.NONE, SearchExtractors.extract(Event("5".repeat(64), alice, 1L, 7, emptyArray(), "+", "")))
    }
}
