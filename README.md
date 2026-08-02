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
| `filter:rank:gte:N` / `filter:rank:gt:N` | Raise the trust floor from the default 2 to N (0–100 scale) — a pure filter, the ordering is untouched. |
| `include:spam` | Turn off the default trust floor. An explicit `filter:rank:` floor always survives it. |

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
| `pizza sort:text` | Pure text relevance even when an observer resolved (token or connection) — the un-personalized view. |
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
   stored 10040 names count, and they are credited per observer — a popular
   provider's cards fan out to every user whose 10040 names it.
3. The store folds the cards into per-subject **reputation tensors**
   (subject → {observer: score}) as they are written — queries never scan
   30382s. At query time the author's cell for the observer is the
   `user_score()` the profiles gate and sort on, so the per-query cost is one
   tensor-cell lookup per candidate, independent of how large the observer's
   network is (300k ranked keys costs the same as 300).

Two consequences worth knowing:

- **Listed is not enough — the score must clear the floor.** A subject the
  service ranked at 0 or 1 is in the d-tag list but below the default floor
  of 2, so the gate drops it.
- **Switching providers is automatic but only as fresh as the stored cards.**
  Replacing a 10040 re-attributes immediately: the old service's scores stop
  counting and the new one's take over, no query-side change needed. But the
  store never fetches the new service's 30382s itself — until they are
  ingested, the observer's score map is empty and, with the observer still
  resolved, gated feeds return **nothing** (the gate fails closed here, unlike
  the no-observer case). When a 10040 changes, sync the named service's 30382
  corpus promptly.

## What's searchable

A search matches on the content and some tags of each event, and different fields
carry different weight: a **primary** field (a title or name) outweighs a
**secondary** field (a summary, description, or hashtags), which outweighs the
**body** (the event's `content`). Profiles (kind 0) are split into their own
name and identity fields. When you supply an observer, the matches are weighted
and spam-gated by that observer's web of trust.

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
| **1010** | note modification / edit | content |
| **1068** | poll (NIP-88) | content |
| **1111** | comment (NIP-22) | content |
| **1163** | profile-gallery entry | content |
| **20** | picture | title, content |
| **21 / 22 / 34235 / 34236** | video (normal / short / horizontal / vertical) | title, content |
| **1063** | file | summary, content |
| **1065** | file-storage header | content |
| **31337** | audio track | subject |
| **1808** | audio header | content |
| **36787** | music track | title, artist + album, content |
| **34139** | music playlist | title, description, content |
| **54 / 10154** | podcast episode / show | title, description, content |
| **30054 / 30055** | Podcasting-2.0 episode / trailer | content |
| **2003** | torrent | title, content |
| **2004** | torrent comment | content |
| **9802** | highlight | comment + context, content |
| **30311 / 1313** | live event / clip | title, summary, content |
| **1311** | live-stream chat message | content |
| **1312** | live-stream raid | content |
| **30617** | git repository | name, description, content |
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
| **30312 / 30313** | meeting space / room | room or title, summary, content |
| **34550** | community | name, description + rules, content |
| **39000** | group | name, about |
| **9002** | group-metadata edit (NIP-29) | content |
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
| **38192** | PlayStation-1 memory-card save | content |
| **30009** | badge | name, description, content |
| **30030** | emoji pack | title, description, content |
| **30017 / 30018 / 30019 / 30020** | marketplace stall / product / config / auction (NIP-15) | content |
| **38383** | P2P order (NIP-69) | content |
| **9041** | zap goal | summary, content |
| **33863** | fundraiser | title, content |
| **9734 / 9735** | zap request / receipt (NIP-57) | content |
| **9321** | nutzap (NIP-61) | content |
| **8333** | onchain zap (NIP-BC) | content |
| **6969** | zap poll | content |
| **9736 / 9737** | BOLT12 zap / intent (NIP-B1) | content |
| **30315** | user status (NIP-38) | content |
| **1985** | label (NIP-32) | content |
| **30000 / 39089** | people list / follow pack | title, description |
| **10003 / 30001 / 30003** | bookmark lists | title, description |
| **30015** | interest set | title, description + hashtags |
| **30004 / 30005 / 30006 / 30063 / 30267** | article / video / picture / release / app curation sets | title, description |
| **30002 / 39092 / 39701** | relay set / media starter pack / web bookmark | title, description |
| **31890** | feed definition | title |
| **30382** | contact card / relationship | content |
| **30296 / 30297** | interactive story prologue / scene | title, summary, content |
| **1301 / 33401** | workout record / exercise template | title, content |
| **5050 / 5100 / 5250** | NIP-90 DVM job requests (text / image / speech generation) | content |
| **5302 / 5303** | NIP-90 content / people search request | content |
| **11871 / 31873** | attestor proficiency / recommendation | content |
| **31871 / 31872** | attestation / attestation request | content |
| **38000** | mint recommendation | content |
| **2473** | bird detection (Birdstar) | content |
| **12473** | Birdex species collection | content |
| **1315** | road event report (Roadstr) | content |

Anything Quartz parses to a `SearchableEvent` is indexed, current or future. The
authoritative mapping is
[`store/…/SearchExtractors.kt`](store/src/main/kotlin/com/vitorpamplona/quartz/eventstore/store/mapping/SearchExtractors.kt).

## Quick start

Vespa is a prerequisite, like a database — stand one up, then point the store at it.

```kotlin
dependencies {
    implementation("com.vitorpamplona.quartz.eventstore:store:1.0.0")

    // Optional: in-memory test doubles (InMemoryEventIndex, MockVespaEngine),
    // so your own tests run with no Vespa instance.
    testImplementation(testFixtures("com.vitorpamplona.quartz.eventstore:vespa:1.0.0"))
}
```

Published to Maven Central under the `com.vitorpamplona.quartz.eventstore` group.

```kotlin
import com.vitorpamplona.quartz.eventstore.store.VespaEventStore

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
`com.github.vitorpamplona.vespa-eventstore:store:<commit>`.

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
  the `:vespa` jar and `open(autoDeploy = true)` deploys it to a fresh Vespa on first
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

The `:vespa` testFixtures (`InMemoryEventIndex`, `MockVespaEngine`) let the tests
run with no Vespa instance up, so `./gradlew test` needs nothing external.

## Publishing

Publishing uses the [vanniktech Maven Publish](https://github.com/vanniktech/gradle-maven-publish-plugin)
plugin (the same one Quartz ships to Central with).

Install GnuPG and generate a key:

```bash
gpg --gen-key
```

Run `gpg --list-keys` to show your GPG keys.

Distribute the public key:

```bash
gpg --keyserver keyserver.ubuntu.com --send-keys <pubkey>
```

Export your private key to a file:

```bash
gpg --export-secret-keys > ~/.gnupg/secring.gpg
```

Generate a User Token on Maven Central.

To publish from local, add the following fields to your `~/.gradle/gradle.properties` file:

```properties
mavenCentralUsername=<maven user>
mavenCentralPassword=<maven password>
signing.keyId=<gpg key id>
signing.password=<gpg key passphrase>
signing.secretKeyRingFile=<yourhome>/.gnupg/secring.gpg
```

Then run:

```bash
./gradlew publishAllPublicationsToMavenCentral --no-configuration-cache
```

To publish from GitHub Actions, export your private key as a base64 string:

```bash
gpg --export-secret-keys --armor <key-id> ~/.gnupg/secring.gpg | grep -v '\-\-' | grep -v '^=.' | tr -d '\n'
```

and add the following secrets to your GitHub secrets:

```properties
SONATYPE_USERNAME=<maven user>
SONATYPE_PASSWORD=<maven password>
SIGNING_PRIVATE_KEY=<base64versionOfTheFile>
SIGNING_PASSWORD=<gpg key passphrase>
```

- **CI** (`.github/workflows/build.yml`) runs `./gradlew build` on every push and PR to `main`.
- **Release** (`.github/workflows/create-release.yml`) publishes to Maven Central on a
  `v*` tag via `./gradlew publishAllPublicationsToMavenCentral`.

Bump the version in `gradle/libs.versions.toml` (`app`), then just tag the release
version starting with `v` (`vX.Y.Z`) and push the tag.

## Contributing

Issues can be logged on [GitHub issues](https://github.com/vitorpamplona/vespa-eventstore/issues). [Pull requests](https://github.com/vitorpamplona/vespa-eventstore/pulls) are very welcome.

By contributing to this repository, you agree to license your work under the MIT license. Any work contributed where you are not the original author must contain its license header with the original author(s) and source.

# Contributors

<a align="center" href="https://github.com/vitorpamplona/vespa-eventstore/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=vitorpamplona/vespa-eventstore" />
</a>

# MIT License

<pre>
Copyright (c) 2026 Vitor Pamplona

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
