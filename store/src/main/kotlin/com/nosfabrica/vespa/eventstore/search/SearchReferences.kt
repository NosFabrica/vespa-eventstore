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
package com.nosfabrica.vespa.eventstore.search

import com.vitorpamplona.quartz.experimental.trustedLists.addressables.AddressableTrustedListEvent
import com.vitorpamplona.quartz.experimental.trustedLists.addressables.tags.AddressMemberTag
import com.vitorpamplona.quartz.experimental.trustedLists.events.EventTrustedListEvent
import com.vitorpamplona.quartz.experimental.trustedLists.events.tags.EventMemberTag
import com.vitorpamplona.quartz.experimental.trustedLists.users.UserTrustedListEvent
import com.vitorpamplona.quartz.experimental.trustedLists.users.tags.PubKeyMemberTag
import com.vitorpamplona.quartz.nip01Core.core.Address
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.core.HexKey
import com.vitorpamplona.quartz.nip01Core.core.Kind
import com.vitorpamplona.quartz.nip01Core.core.TagArray
import com.vitorpamplona.quartz.nip01Core.tags.aTag.ATag
import com.vitorpamplona.quartz.nip01Core.tags.dTag.dTag
import com.vitorpamplona.quartz.nip01Core.tags.events.ETag
import com.vitorpamplona.quartz.nip01Core.tags.people.PTag
import com.vitorpamplona.quartz.nip32Labeling.LabelEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.addressables.AddressableAssertionEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.events.EventAssertionEvent
import com.vitorpamplona.quartz.nip85TrustedAssertions.users.ContactCardEvent
import com.vitorpamplona.quartz.utils.Hex

/**
 * What a search hit SAYS SOMETHING ABOUT: the records a Trusted List, a NIP-85
 * Trusted Assertion or a NIP-32 label nominates as its subjects.
 *
 * These three families are the kinds whose text is *about* something else. A
 * list is found by its title, an assertion by its petname or summary, a label
 * by its label value — and in every case the thing the reader actually wants
 * is the record on the other end of the pointer, which carries none of the
 * matched text and so can never be recalled by the same search.
 * [SearchReferenceExpansion] is what puts it in the feed beside its pointer.
 *
 * ## Dispatch is on the KIND, never on the runtime class
 *
 * Every accessor below is a tag read Quartz already publishes, but reached
 * through the kind rather than through `is UserTrustedListEvent`. That is
 * deliberate and it is the trap this file is written around: a CONSUMER may
 * force its own quartz over the one this module asks for (vespa-relay does,
 * with `resolutionStrategy { force(libs.quartz) }`), so the class this code
 * runs against is not necessarily the one it compiled against. A quartz
 * predating the Trusted List family has no `EventFactory` branch
 * for kinds 30392-30395 at all. `toEvent()` then hands back a base [Event],
 * every `is` check goes false, and the expansion quietly stops happening with
 * nothing anywhere throwing. Reading the tags off the KIND cannot fail that
 * way — and it matters more here than it would in a consumer, because this
 * module's quartz pin is the one every consumer is forced onto.
 *
 * ## Which tag holds the subject, per family
 *
 * | kind | family | subject |
 * |---|---|---|
 * | 1985 | NIP-32 label | every `e` / `p` / `a` tag (`r` and `t` are not nostr records) |
 * | 30382-30384 | NIP-85 assertion | the `d` tag: a pubkey, an event id, an a-coordinate |
 * | 30392-30394 | Trusted List | the member tag this kind's last digit denotes: `p` / `e` / `a` |
 *
 * The 5-suffixed pair (30385, 30395) carries NIP-73 EXTERNAL identifiers —
 * urls, ISBNs, podcast guids. There is no nostr event to add for those, so
 * they are absent from [KINDS] AND from [DECLARATIONS], which is load-bearing
 * twice over: a row of those kinds is never materialized to read tags that
 * cannot name an event, and a REQ that asks for nothing else never reaches the
 * page-collecting path or pays the enrolment lookup that gates a family it
 * cannot expand.
 *
 * A Trusted List's OTHER reference tags are metadata, not membership, and are
 * deliberately not read: `aboutAddresses()` / `aboutPubKeys()` say what the
 * list is *for* (the tag coordinate it was computed over, the observer it was
 * computed under), so a 30393 of notes would otherwise drag its observer's
 * profile into the feed as if the list had nominated it. Dispatching on the
 * kind and reading only that kind's member tag gets this right by construction.
 */
internal object SearchReferences {
    /**
     * The Trusted List and Trusted Assertion families — the kinds that only
     * expand for a reader who NAMED their signer.
     *
     * These are a trust provider's computed OUTPUT, and a NIP-85 reader
     * chooses their providers by publishing a kind-10040 that names them. A
     * list from a service nobody named is a stranger's computation, and
     * splicing its members into a feed would put it in front of a reader as
     * if they had asked for it — which is the one thing a web-of-trust relay
     * must not do on their behalf. [SearchReferenceExpansion] resolves the
     * reader's own 10040 and admits only those signers, plus the reader.
     *
     * A NIP-32 label (1985) is deliberately NOT in here. A label is a
     * first-class public annotation that anyone may publish about anything —
     * distributed moderation is the NIP's stated purpose — and it is not a
     * provider's machinery that only means something to the reader who
     * enrolled it. It also had to survive this relay's trust-ranked search to
     * be a hit at all, which is the gate that already applies to it.
     */
    val DECLARATIONS: Set<Kind> =
        setOf(
            ContactCardEvent.KIND,
            EventAssertionEvent.KIND,
            AddressableAssertionEvent.KIND,
            UserTrustedListEvent.KIND,
            EventTrustedListEvent.KIND,
            AddressableTrustedListEvent.KIND,
        )

    /** Whether [kind] is a Trusted List or Trusted Assertion, and so gated on its signer. */
    fun isDeclaration(kind: Kind) = kind in DECLARATIONS

    /**
     * The kinds [of] can extract a subject from. This is the whole gate on the
     * zero-decode read path: a [com.vitorpamplona.quartz.nip01Core.store.RawEvent]
     * carries its kind as a field, so a replay row is only materialized —
     * tags parse, `EventFactory` dispatch — when its kind is in here.
     */
    val KINDS: Set<Kind> = DECLARATIONS + LabelEvent.KIND

    /** The subjects of [event], or [References.NONE] for a kind that nominates nothing. */
    fun of(event: Event): References =
        when (event.kind) {
            LabelEvent.KIND -> {
                References(
                    eventIds = event.tags.mapNotNull(ETag::parseId),
                    pubKeys = event.tags.mapNotNull(PTag::parseKey),
                    addresses = event.tags.mapNotNull { canonical(ATag.parseAddressId(it)) },
                )
            }

            ContactCardEvent.KIND -> {
                References(pubKeys = listOfNotNull(event.tags.subjectKey()))
            }

            EventAssertionEvent.KIND -> {
                References(eventIds = listOfNotNull(event.tags.subjectKey()))
            }

            AddressableAssertionEvent.KIND -> {
                References(addresses = listOfNotNull(event.tags.subjectAddress()))
            }

            UserTrustedListEvent.KIND -> {
                References(
                    pubKeys = event.tags.mapNotNull(PubKeyMemberTag::parseKey),
                    confidence = event.tags.memberScores(PubKeyMemberTag.TAG_NAME, PubKeyMemberTag::parseKey),
                )
            }

            EventTrustedListEvent.KIND -> {
                References(
                    eventIds = event.tags.mapNotNull(EventMemberTag::parseId),
                    confidence = event.tags.memberScores(EventMemberTag.TAG_NAME, EventMemberTag::parseId),
                )
            }

            AddressableTrustedListEvent.KIND -> {
                References(
                    addresses = event.tags.mapNotNull { canonical(AddressMemberTag.parseAddressId(it)) },
                    // Keyed by the CANONICAL coordinate, since that is what the
                    // lookup files a found subject under — a member written as
                    // `naddr1…` or with an upper-case key would otherwise carry
                    // a score nothing could look up.
                    confidence = event.tags.memberScores(AddressMemberTag.TAG_NAME) { canonical(AddressMemberTag.parseAddressId(it)) },
                )
            }

            else -> {
                References.NONE
            }
        }

    /**
     * An assertion's `d` tag read as a 64-hex key. Rejected rather than looked
     * up when it is anything else: the `d` of a well-formed 30382 IS the
     * subject pubkey, so a value that is not one is a malformed assertion, and
     * asking the store for it would cost a query that can only miss.
     */
    private fun TagArray.subjectKey(): HexKey? = dTag().takeIf { it.length == 64 && Hex.isHex64(it) }

    /** An assertion's `d` tag read as an a-coordinate, `kind:pubkey:d`. */
    private fun TagArray.subjectAddress(): String? = canonical(dTag())

    /**
     * An `a` value as `kind:lowercase-hex:d`, or null when it names no
     * addressable event.
     *
     * NORMALIZED HERE so both sides of the match are the same string. The
     * lookup recalls by (kind, author, `d`) and the result is recognized again
     * by reassembling the coordinate from the event's OWN fields, which are
     * canonical — so a member published as `naddr1…` (which `Address.parse`
     * decodes) or with an upper-case pubkey would be fetched and then silently
     * dropped for failing to equal its own raw tag string. Nostr hex is
     * lower-case by convention and the store holds it that way, so the
     * publisher's spelling is the side that gives.
     */
    private fun canonical(raw: String?): String? = raw?.let { Address.parse(it) }?.let { Address.assemble(it.kind, it.pubKeyHex.lowercase(), it.dTag) }
}

/**
 * The three ways a nostr record can be named, kept apart because each is
 * recalled by a different filter shape: an id, a `kind:pubkey:d` coordinate,
 * or a pubkey — which resolves to that author's kind-0 profile, since a
 * pubkey names a person rather than an event.
 */
internal class References(
    val eventIds: List<HexKey> = emptyList(),
    val pubKeys: List<HexKey> = emptyList(),
    val addresses: List<String> = emptyList(),
    /**
     * HOW SURE THE POINTER IS, per reference, on quartz's 0..100 scale — empty
     * for every pointer that expresses no confidence at all.
     *
     * Only Trusted List membership carries one. A NIP-32 label has no
     * confidence field in the NIP, and a NIP-85 assertion's `d` IS its subject,
     * so neither claim is probabilistic: an absent entry means "as sure as the
     * pointer itself", not "unsure". Placement reads it that way.
     *
     * Out of 0..100 reads back as ABSENT rather than clamped, which is quartz's
     * own rule for the same tag: a publisher counting on another scale is
     * unscored, and pinning a 950 to 100 would rank that member above every
     * honestly-scored peer.
     */
    val confidence: Map<String, Int> = emptyMap(),
) {
    val size: Int get() = eventIds.size + pubKeys.size + addresses.size

    fun isEmpty() = size == 0

    /** [confidence] for [key] as a 0..1 weight, or null where the pointer said nothing. */
    fun weightOf(key: String): Double? = confidence[key]?.let { it / 100.0 }

    companion object {
        val NONE = References()
    }
}

/** The 0..100 percentage at index 3 of a member tag, or null where the scale cannot express it. */
private fun scoreOf(tag: Array<String>): Int? = tag.getOrNull(3)?.toIntOrNull()?.takeIf { it in 0..100 }

/** `value -> score` for every member tag of [name] that carries a readable one. */
internal fun TagArray.memberScores(
    name: String,
    valueOf: (Array<String>) -> String?,
): Map<String, Int> =
    buildMap {
        this@memberScores.forEach { tag ->
            if (tag.size > 1 && tag[0] == name) {
                val value = valueOf(tag) ?: return@forEach
                scoreOf(tag)?.let { put(value, it) }
            }
        }
    }
