# vespa-eventstore

A [Vespa](https://vespa.ai)-backed [Quartz](https://github.com/vitorpamplona/amethyst) Event Store that filters and ranks everything — REQs, COUNTs, and NIP-50 search — through each connecting user's **NIP-85 web of trust**.

## Features

- **NIP-01 Storage & Retrieval**
    - Stores Nostr events and retrieves them using Nostr filters.

- **Replaceable Events**
    - Enforces a unique constraint by kind and pubkey.
    - Old versions are removed when newer versions arrive.
    - Old versions are blocked if newer versions already exist.
    - Same `created_at`: NIP-01 lexical-id tiebreaker (lowest id wins).

- **Addressable Events**
    - Enforces a unique constraint by kind, pubkey, and d-tag.
    - Old versions are removed when newer versions arrive.
    - Old versions are blocked if newer versions already exist.
    - Same `created_at`: NIP-01 lexical-id tiebreaker (lowest id wins).

- **Ephemeral Events**
    - Ephemeral events are never stored.

- **NIP-40 Expirations**
    - Deletes expired events.
    - Blocks expired events from being re-inserted.

- **NIP-09 Deletion Events**
    - Deletes by event id.
    - Deletes by address up to and including the deletion's `created_at` (newer versions are kept).
    - Blocks deleted events from being re-inserted.
    - Only the original author's deletions take effect; cross-author kind-5s are stored but inert.
    - GiftWraps are deleted by p-tag.

- **NIP-62 Right to Vanish**
    - Deletes all of a user's events up to the request's `created_at`.
    - Blocks vanished events from being re-inserted.
    - GiftWraps are deleted by p-tag.

- **NIP-45 Counts**
    - Counts records matching Nostr filters.

- **NIP-50 Full Text Search**
    - Banded BM25 relevance (match-quality tiers, IDF weighting, trigram typo-recall) ranks results.
    - Observer-centric ranking weights results by NIP-85 user scores.
    - The observer gate: a resolved observer turns plain recall into a trusted-only feed (`include:spam` opts out).
    - Indexes are updated on replaceables, deletions, vanishes, and expirations.

- **NIP-77 Negentropy**
    - Exposes negentropy id snapshots so a relay built on the store can reconcile with peers.

- **NIP-91 AND operator for tags**
    - Matches events carrying two or more required tags at the same time.

### Search grammar

Extensions travel inside the `search` string. `sort:` picks the ordering,
`filter:` moves the trust floor, `include:spam` removes it — the three are
orthogonal and stack freely. Unknown extensions are ignored, and `scheme://…`
tokens stay part of the search text:

| Token | Effect |
|---|---|
| `observer:<64-hex>` | Rank through this pubkey's web of trust. With no observer resolved (see below) trust ranking is impossible: searches fall back to pure-text relevance and every trust token below quietly no-ops. |
| `sort:rank` / `sort:rank:asc` | Trust-order within match tiers (descending / ascending). |
| `sort:followers` | Verified-follower-count order within match tiers. |
| `sort:text` | Force pure-text relevance, ignoring the observer. |
| `sort:recent` | Chronological search: the same match set, ordered `created_at desc` like a plain NIP-01 filter, still gated by the observer's floor. Match quality is not consulted — a weak match from a minute ago sits above a perfect one from yesterday. |
| `filter:rank:gte:N` / `filter:rank:gt:N` | Raise the trust floor from the default 2 to N (0–100 scale) — a pure filter, the ordering is untouched. |
| `include:spam` | Turn off the default trust floor. An explicit `filter:rank:` floor always survives it. |
| `-word` | Google-style exclusion: drop hits containing the word. Exact-match only — the typo/prefix tolerance the positive terms enjoy never widens an exclusion (though the prose fields stem, so `-runs` also drops `running` there) — and hyphenated exclusions (`-e-cash`) exclude the adjacent phrase. A query of *only* exclusions is plain newest-first recall minus the words (the observer gate still applies), and events search can't see (non-searchable kinds) are never excluded. |
| `:shortcode:` | A NIP-30 custom emoji, searched as the picture it is: a word that is entirely a shortcode matches the accounts and events whose own `emoji` tags DECLARE that badge, never the word inside it (and `verified` never matches the badge). Undeclared runs are ordinary text, so a clock (`8:30:45`) is untouched; quoting (`":verified:"`) asks for the literal text instead. `-:verified:` excludes badge wearers. |
| `"exact phrase"` | Google-style quotes: the words must appear adjacently, in order. Exact-match only — quoting a single word (`"vitor"`) is the opt-out from typo/prefix matching (not from prose-field stemming). Unlike exclusions, phrases are search text: a phrase-only query is a relevance-ranked search with the normal trust/spam treatment. `-"exact phrase"` excludes the phrase. An unclosed quote runs to the end; quotes protect extension-shaped tokens (`"sort:rank"` is a phrase, not a sort). |

**Where the observer comes from.** The `observer:` token is only one of two
sources. The embedding relay can also supply an observer out-of-band — Quartz's
`StoreQueryContext` coroutine context element — which is how "this connection is
NIP-42-authenticated as X" (or an operator-wide default lens) reaches the store
without touching the client's query. An explicit `observer:` token wins over the
context observer: scores are public, so any client may rank through any lens.
Every behavior below keys off the *resolved* observer, whichever source it came
from — on an authenticated connection, `pizza` behaves like
`pizza observer:<your-hex>`, and `sort:text` is how that connection opts back
out of personalization.

**The observer gate.** Supplying an observer — either way — opts the *whole
request* into that lens, plain NIP-01 filters included: non-search queries keep
their newest-first order but drop authors the observer trusts below the floor
(2 by default, `filter:rank:` to move it, `include:spam` to lift it). Recall
without a resolved observer is never gated — an anonymous REQ sees everything —
and sync paths (negentropy snapshots, internal sweeps) never resolve one.

How they combine (`<hex>` = a 64-hex observer pubkey; the examples assume no
context observer, so the token is the only source):

| Example `search` string | What you get |
|---|---|
| *(no `search` field)* | Plain NIP-01: newest first, no ranking, no gate. |
| `observer:<hex>` *(no terms)* | The observer gate: newest first, but only authors the observer trusts ≥ 2. (An authenticated connection gets this on every plain filter without any search string.) |
| `include:spam` *(no terms)* | Opts a plain filter back out of the gate — full ungated recall even with an observer resolved. |
| `pizza` | Pure text relevance — no observer resolved, so no trust and no spam floor. |
| `pizza observer:<hex>` | **The default:** text score × the observer's web-of-trust curve; authors below trust 2 dropped as spam. |
| `pizza observer:<hex> include:spam` | Same order, floor off — low-trust authors rank low instead of disappearing. |
| `pizza observer:<hex> filter:rank:gte:20` | Same default order, floor raised to 20. |
| `pizza observer:<hex> sort:rank` | Token matches first, ordered by author trust inside each match tier. |
| `pizza observer:<hex> sort:followers` | Token matches first, most-followed authors first inside each tier. |
| `pizza -pineapple` | Text relevance for `pizza`, minus every hit that contains `pineapple`. |
| `-pineapple observer:<hex>` | No terms left: the observer-gated feed, minus every searchable hit containing `pineapple`. |
| `"new york" pizza` | Hits must contain the adjacent phrase `new york`; `pizza` still matches loosely and drives relevance alongside it. |
| `"pizza" observer:<hex>` | Exactly the token `pizza` (typo/prefix matching off), ranked through the observer's web of trust like any search. |
| `pizza -"pineapple ham"` | `pizza` hits, minus those containing the adjacent phrase `pineapple ham` — `pineapple` or `ham` alone is fine. |
| `pizza sort:text` | Pure text relevance even when an observer resolved (token or connection) — the un-personalized view. |
| `pizza observer:<hex> sort:recent` | Every `pizza` hit from an author trusted ≥ 2, newest first — search results ordered exactly like a feed. |
| `pizza sort:recent` | No observer: the same chronological page, ungated. |
| `sort:rank observer:<hex>` | No terms: the trust firehose — everything, ordered by author trust. |
| `filter:rank:gte:50 observer:<hex>` | No terms: newest-first feed of authors the observer trusts at ≥ 50. |

### Where trust comes from (NIP-85)

The scores behind every trust behavior above are NIP-85 events the store
ingests like any other:

1. The observer's **kind 10040** names a trust provider — a service pubkey.
   The entry is per dimension: `30382:rank` picks the service whose scores
   gate and order (`sort:rank`, the floors, the observer gate), and
   `30382:followers` may name a *different* service for `sort:followers`.
2. That service signs **kind 30382** cards: the d-tag is the subject pubkey,
   the rank tag its 0–100 score. Only cards signed by a service that some
   stored 10040 names are projected; a popular provider's cards are stored
   once, under its own key, however many users name it.
3. The store folds the cards into per-subject **reputation tensors**
   (subject → {service: score}) as they are written, one cell per card —
   queries never scan 30382s. At query time the observer's 10040 is resolved
   to the service it names per dimension (the lens), and the author's cell
   under that service is the `user_score()` the profiles gate and sort on, so
   the per-query cost is one tensor-cell lookup per candidate, independent of
   how large the observer's network is (300k ranked keys costs the same as
   300). Re-signing a 10040, or pointing it at a provider some list already
   names, writes nothing; see `docs/service-keyed-trust.md`.

Three consequences worth knowing:

- **Listed is not enough — the score must clear the floor.** A subject the
  service ranked at 0 or 1 is in the d-tag list but below the default floor
  of 2, so the gate drops it.
- **Switching providers is a query-time resolution, but only as fresh as the
  stored cards.** Replacing a 10040 moves the lens at once: the old service's
  scores stop counting and the new one's take over with no cell rewritten. A
  service no list named before has its stored cards walked into cells once
  (minutes for a 300k-card key, in the background). But the store never
  fetches the new service's 30382s itself — until they are ingested, the
  observer's lens finds no cells and, with the observer still resolved, gated
  feeds return **nothing** (the gate fails closed here, unlike the no-observer
  case). When a 10040 changes, sync the named service's 30382 corpus promptly.
- **Cards from a service nobody named are dead storage.** A mirror that syncs
  30382s by kind pulls every scoring service on the network, not just the ones
  its users trust, and those cards can never become a cell for any observer.
  `store.sweepOrphanScores()` deletes them (`dryRun = true` first, to see how
  much it would free). It is an operator action, never automatic, and it
  refuses to run at all while no 10040 is readable — that state is
  indistinguishable from a corpus mirrored before its provider lists, where
  sweeping would delete every score you hold.

## What's searchable

A search matches on the content and some tags of each event, and different fields
carry different weight: a **primary** field (a title or name) outweighs a
**secondary** field (a summary, description, or hashtags), which outweighs the
**body** (the event's `content`). Profiles (kind 0) are split into their own
name and identity fields. When you supply an observer, the matches are weighted
and spam-gated by that observer's web of trust.

Matching is **language-neutral**: text is folded and tokenized but never stemmed,
so a note is findable by its own words whatever language it is written in. (The
engine stems a document by the language it *detects* while queries carry none, so
stemming would silently make every non-English note unreachable — see the
`stemming: none` note in `schemas/event.sd`.) Scripts the engine does not
segment, CJK above all, used to be unreachable inside a body entirely. They are
now reachable **from four characters up**, through the body's trigram index —
trigrams need no word segmentation, so they route around the gap rather than
closing it (the exact path still cannot reach inside an unsegmented run). Read
that as half a fix, not a fix: four characters is two trigrams, the minimum a
trigram phrase can be, and a great deal of ordinary Chinese and Japanese
vocabulary is *two* characters — 東京, 会社 — which yields no trigrams at all and
stays unreachable in a body. CJK *names* are unaffected and always worked, since
they ride the prefix attributes rather than the tokenizer.

**Partial words** reach every field, but by two different routes. Names, titles,
subjects, hashtags and identity handles carry prefix and typo matching (`vitorp`
finds *Vitor Pamplona*, one typo is forgiven), served by attribute columns. A
**body** — a note's content, an article's prose — is reached by substring instead
(`testin` finds *Testing*), served by a trigram index and matched as a phrase, so
it covers the whole post at any length with no cap on how deep the word sits. The
trade is that a body match is a substring rather than an anchored prefix, so
`testin` also finds *Protesting*, and typos in a body are not forgiven. A body is
deliberately given no attribute column: it is filled on nearly every document, so
one would cost more RAM than the entire rest of the schema and would still only
reach a post's first few dozen words — see `docs/attribute-memory.md`.

The kinds it indexes and the fields it reads from each (highest weight first).
Kinds with no title to split out are indexed by their full `content`; on every
kind, hashtags also fold into the secondary tier and `location` tags into the
place column:

| Kind(s) | What it is | Indexed fields |
| --- | --- | --- |
| **0** | profile | name, display name, about, NIP-05, lightning address, website |
| **1** | note | subject, hashtags, content |
| **9** | relay chat message (NIP-C7) | content |
| **11** | thread | title, content |
| **14** | private chat message (NIP-17) | content |
| **24** | public message | content |
| **40 / 41** | public chat channel | name, about |
| **42** | public-chat channel message (NIP-28) | content |
| **3302** | chat message edit (Concord CORD-02) | content |
| **40002** | Buzz stream chat message | content |
| **45001 / 45003** | Buzz forum post / comment | content |
| **1010** | note modification / edit | summary, content |
| **1068** | poll (NIP-88) | option labels, content |
| **1111** | comment (NIP-22) | content |
| **1163** | profile-gallery entry | summary |
| **20** | picture | title, content |
| **21 / 22 / 34235 / 34236** | video (normal / short / horizontal / vertical) | title, content |
| **1063** | file | summary, content |
| **1065** | file-storage header | summary |
| **31337** | audio track | subject |
| **1808** | audio header | content |
| **36787** | music track | title, artist + album, content |
| **34139** | music playlist | title, description, content |
| **54 / 10154** | podcast episode / show | title, description, content |
| **30054 / 30055** | Podcasting-2.0 episode / trailer | title, description, content |
| **2003** | torrent | title, content |
| **2004** | torrent comment | content |
| **9802** | highlight | comment + context, content |
| **30311 / 1313** | live event / clip | title, summary, content |
| **1311** | live-stream chat message | content |
| **1312** | live-stream raid | content |
| **30617** | git repository | name, description, content, homepage + clone URLs |
| **1621 / 1618** | git issue / pull request | subject, content |
| **1617** | git patch | content |
| **1622** | git reply | content |
| **1630 / 1631 / 1632 / 1633** | git status (open / applied / closed / draft) | content |
| **1337** | code snippet | name, description, content |
| **30817** | NIP-on-Nostr document | title, content |
| **32267** | software application | name, summary, content |
| **30023** | long-form article | title, summary + hashtags, content |
| **30818** | wiki article | title, summary, content |
| **30402** | classified listing | title, summary, content |
| **31924 / 31922 / 31923** | calendar & slots | title, summary, content |
| **31925** | calendar RSVP | content |
| **30312 / 30313** | meeting space / room | room or title, summary, content, streaming endpoint |
| **34550** | community | name, description + rules, content |
| **39000** | group | name, about |
| **9002** | group-metadata edit (NIP-29) | name, about |
| **31990** | app handler | name + display name, about |
| **10100** | Buzz agent profile | name, display name |
| **30175** | Buzz agent persona | display name, system prompt |
| **30177** | Buzz managed agent | name, system prompt |
| **30176** | Buzz agent team | name, description, instructions |
| **30620** | Buzz workflow definition | name, content |
| **40100** | Buzz channel canvas | content |
| **48106** | Buzz huddle guidelines | content |
| **15128 / 35128** | website | title, description |
| **15129 / 35129 / 5129** | napplet root / named / snapshot | title, description |
| **38192** | PlayStation-1 memory-card save | title, summary + region + filename |
| **30009** | badge | name, description, content |
| **30030** | emoji pack | title, description, content |
| **30017 / 30018 / 30019 / 30020** | marketplace stall / product / config / auction (NIP-15) | name, description |
| **38383** | P2P order (NIP-69) | maker name, currency + payment methods |
| **9041** | zap goal | summary, content |
| **33863** | fundraiser | title, content |
| **9734 / 9735** | zap request / receipt (NIP-57) | content |
| **9321** | nutzap (NIP-61) | content |
| **8333** | onchain zap (NIP-BC) | content |
| **6969** | zap poll | option labels, content |
| **9736 / 9737** | BOLT12 zap / intent (NIP-B1) | content |
| **30315** | user status (NIP-38) | content |
| **1985** | label (NIP-32) | label values, content |
| **30000 / 39089** | people list / follow pack | title, description |
| **10003 / 30001 / 30003** | bookmark lists | title, description |
| **30015** | interest set | title, description + hashtags |
| **30004 / 30005 / 30006 / 30063 / 30267** | article / video / picture / release / app curation sets | title, description |
| **30002 / 39092 / 39701** | relay set / media starter pack / web bookmark | title, description |
| **31890** | feed definition | title |
| **30382** | contact card / relationship | petname, summary + topics as hashtags (the encrypted half is never indexed) |
| **30392 / 30393 / 30394 / 30395** | trusted list of pubkeys / events / addressables / external ids | title |
| **30296 / 30297** | interactive story prologue / scene | title, summary, content |
| **1301 / 33401** | workout record / exercise template | title, content |
| **5050 / 5100 / 5250** | NIP-90 DVM job requests (text / image / speech generation) | content |
| **5302 / 5303** | NIP-90 content / people search request | content |
| **11871 / 31873** | attestor proficiency / recommendation | content |
| **31871 / 31872** | attestation / attestation request | content |
| **38000** | mint recommendation | content |
| **2473** | bird detection (Birdstar) | species + common name, alt |
| **12473** | Birdex species collection | summary + species names |
| **1315** | road event report (Roadstr) | content |

Anything Quartz parses to a `SearchableEvent` is indexed, current or future. The
authoritative mapping is
[`store/…/SearchExtractors.kt`](store/src/main/kotlin/com/nosfabrica/vespa/eventstore/mapping/SearchExtractors.kt).

A NIP-30 `:shortcode:` the event itself declares is not text in any of these
roles: it leaves the field it decorated and indexes as one synthetic term on the
secondary tier, so a badge is searchable as a badge (`:verified:`) and never as
the word inside it. Extraction is derived data — a corpus fed before a change
here is repaired by `reindexFullTextSearch()`, with no resync.

## Quick start

Vespa is a prerequisite, like a database — stand one up, then point the store at it.

```kotlin
dependencies {
    implementation("com.nosfabrica.vespa.eventstore:store:1.0.0")

    // Optional: the wire-level test double (MockVespaEngine); InMemoryEventIndex ships in the engine artifact itself,
    // so your own tests run with no Vespa instance.
    testImplementation(testFixtures("com.nosfabrica.vespa.eventstore:engine:1.0.0"))
}
```

Published to Maven Central under the `com.nosfabrica.vespa.eventstore` group.

```kotlin
import com.nosfabrica.vespa.eventstore.VespaEventStore

// Connects, and on a fresh Vespa deploys the bundled schema (autoDeploy, default on).
VespaEventStore.open("http://localhost:8080").use { store ->

    // Store anything — signed events OR unsigned rumors. The store never verifies
    // signatures (many Nostr events are rumors); verify at your ingress if you need to.
    store.insert(event)
    store.batchInsert(events)          // bulk fast path

    // Plain NIP-01 filter — newest first.
    store.query<Event>(Filter(kinds = listOf(1), authors = listOf(pk)))

    // NIP-50 search — pure-text relevance, no trust needed.
    store.query<Event>(Filter(kinds = listOf(0), search = "vitor"))

    // Trust-ranked — just name the observer lens in the search string.
    store.query<Event>(Filter(kinds = listOf(0), search = "vitor observer:<64-hex>"))

    // Or supply the lens out-of-band (how a relay passes the NIP-42 login):
    // searches rank through it, plain filters become trusted-only feeds (the
    // observer gate — newest first, below-floor authors dropped; include:spam
    // opts a query out), and an explicit observer: token overrides it.
    withContext(StoreQueryContext(setOf(authedPubkey))) {
        store.query<Event>(Filter(kinds = listOf(0), search = "vitor"))
    }
}
```

For a commit snapshot, JitPack works:
`com.github.NosFabrica.vespa-eventstore:store:<commit>`.

## Three things to know

- **Supplying an observer gates recall.** A query with a resolved observer —
  `observer:` token or `StoreQueryContext` — only returns authors that lens
  trusts at the floor or above, plain filters included (newest-first order is
  kept). Don't pass a lens on reads that must see everything; `include:spam`
  opts a single query out. On a serving cluster whose schema predates the
  gate's rank profiles, the gate fails open (plain ungated recall) until the
  schema is redeployed — `deployIfAbsent` never redeploys on its own.
- **The store never verifies signatures.** It stores whatever you hand it — signed
  events *and* unsigned rumors (NIP-59 inner events, drafts). Verifying signed
  network input is the caller's job, at ingress.
- **The schema ships with the code.** The Vespa application package is bundled into
  the `:engine` jar and `open(autoDeploy = true)` deploys it to a fresh Vespa on first
  run — so the schema and the query builder can never drift. An operator who owns
  deployment out of band can pass `autoDeploy = false` — that is also the path to a
  multi-node cluster: [`docs/scaling.md`](docs/scaling.md) is the operator guide
  (own the topology, keep the schemas verbatim, name every container endpoint in
  `open(endpoints = …)`).

## Developer Setup

Make sure to have the following pre-requisites installed:
1. Java 21+ (JDK 21)
2. IntelliJ IDEA CE or Android Studio

Kotlin 2.4 / JDK 21. Quartz comes from JitPack, pinned by commit in
`gradle/libs.versions.toml`.

On first build the project installs the repo's git hooks (`.git-hooks` →
`.git/hooks`) so [Spotless](https://github.com/diffplug/spotless) runs on commit
and the tests run on push, mirroring Amethyst.

## Building

Build the library (compile + tests + `spotlessCheck`):
```bash
./gradlew build
```

Fix the formatting issues the linter flags:
```bash
./gradlew spotlessApply
```

## Testing

Run the full test suite:
```bash
./gradlew test
```

`InMemoryEventIndex` (in the engine artifact) and the `:engine` testFixtures (`MockVespaEngine`) let the tests
run with no Vespa instance up, so `./gradlew test` needs nothing external.

## Publishing

Publishing uses the [vanniktech Maven Publish](https://github.com/vanniktech/gradle-maven-publish-plugin)
plugin (the same one Quartz ships to Central with).

Install GnuPG and generate a signing key. Use `--full-generate-key`, not
`--gen-key`: only the former lets you choose RSA 4096 and an expiry.

```bash
gpg --full-generate-key
```

Then read back the *long* key id — `gpg --list-keys` prints a short form that
the commands below will not accept:

```bash
gpg --list-secret-keys --keyid-format=long
```

Distribute the public key. Central verifies each signature against the public
keyservers, so a release fails until this has propagated (a few minutes):

```bash
gpg --keyserver keyserver.ubuntu.com --send-keys <key-id>
```

Generate a **User Token** on Maven Central (Portal → View Account → Generate
User Token). This is not your portal login — the plugin wants the token's
generated username/password pair, and the login pair fails with a 401.

Export the private key. Both the full armored block and a stripped single-line
form are accepted: the plugin passes the value straight to Gradle's
`useInMemoryPgpKeys`, which parses either.

```bash
# Full armored block — for GitHub secrets, which hold multi-line values fine.
gpg --export-secret-keys --armor <key-id>

# Stripped to one line — for gradle.properties, which cannot hold a multi-line
# value. Drops the BEGIN/END armor lines and the trailing CRC.
gpg --export-secret-keys --armor <key-id> | grep -v '^-----' | grep -v '^=' | tr -d '\n'
```

To publish from local, add the following fields to your `~/.gradle/gradle.properties` file:

```properties
mavenCentralUsername=<token username>
mavenCentralPassword=<token password>
signingInMemoryKey=<private key, stripped to one line>
signingInMemoryKeyPassword=<gpg key passphrase>
```

Then run:

```bash
./gradlew publishAllPublicationsToMavenCentral --no-configuration-cache
```

To publish from GitHub Actions, add the same four values as repository secrets:

```properties
SONATYPE_USERNAME=<token username>
SONATYPE_PASSWORD=<token password>
SIGNING_PRIVATE_KEY=<private key, armored>
SIGNING_PASSWORD=<gpg key passphrase>
```

GitHub secrets are write-only — you cannot read one back out. Keep the private
key and its passphrase somewhere recoverable, or a future release will have to
be signed by a new key. (That is survivable: Central does not require the same
key across releases, and already-published artifacts stay verifiable as long as
the old public key remains on the keyservers.)

- **CI** (`.github/workflows/build.yml`) runs `./gradlew build` on every push and PR to `main`.
- **Release** (`.github/workflows/create-release.yml`) publishes to Maven Central on a
  `v*` tag via `./gradlew publishAllPublicationsToMavenCentral`.

Bump the version in `gradle/libs.versions.toml` (`app`), then just tag the release
version starting with `v` (`vX.Y.Z`) and push the tag.

## Contributing

Issues can be logged on [GitHub issues](https://github.com/NosFabrica/vespa-eventstore/issues). [Pull requests](https://github.com/NosFabrica/vespa-eventstore/pulls) are very welcome.

By contributing to this repository, you agree to license your work under the MIT license. Any work contributed where you are not the original author must contain its license header with the original author(s) and source.

# Contributors

<a align="center" href="https://github.com/NosFabrica/vespa-eventstore/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=NosFabrica/vespa-eventstore" />
</a>

# MIT License

<pre>
Copyright (c) 2026 NosFabrica

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
</pre>
