# vespa-eventstore

A [Vespa](https://vespa.ai)-backed [Quartz](https://github.com/vitorpamplona/amethyst) Event Store with **trust-ranked NIP-50 search** based on NIP-85 events.

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
    - Indexes are updated on replaceables, deletions, vanishes, and expirations.

- **NIP-77 Negentropy**
    - Exposes negentropy id snapshots so a relay built on the store can reconcile with peers.

- **NIP-91 AND operator for tags**
    - Matches events carrying two or more required tags at the same time.

### Search grammar

Extensions travel inside the `search` string:

| Token | Effect |
|---|---|
| `observer:<64-hex>` | Rank through this pubkey's web of trust. Absent ⇒ pure-text relevance, and every trust token below quietly no-ops. |
| `sort:rank` / `sort:rank:asc` / `sort:followers` | Trust-order within match tiers. |
| `sort:text` | Force pure-text relevance. |
| `filter:rank:gte:N` / `filter:rank:gt:N` | Keep only authors the observer trusts ≥ N (0–100 scale). |
| `include:spam` | Turn off the default trust floor that a trust-ranked query applies. |

## What's searchable

A search matches on the content and some tags of each event, and different fields
carry different weight: a **primary** field (a title or name) outweighs a
**secondary** field (a summary, description, or hashtags), which outweighs the
**body** (the event's `content`). Profiles (kind 0) are split into their own
name and identity fields. When you supply an observer, the matches are then
ordered by that observer's web of trust.

The kinds it indexes and the fields it reads from each (highest weight first):

| Kind(s) | What it is | Indexed fields |
| --- | --- | --- |
| **0** | profile | name, display name, about, NIP-05, lightning address, website |
| **1** | note | subject, hashtags, content |
| **11** | thread | title, content |
| **30023** | long-form article | title, summary + hashtags, content |
| **30818** | wiki article | title, summary, content |
| **30402** | classified listing | title, summary, content |
| **9802** | highlight | comment + context, content |
| **20** | picture | title, content |
| **21 / 22** | video | title, content |
| **1063** | file | summary, content |
| **2003** | torrent | title, content |
| **1301 / 33401** | workout record / exercise template | title, content |
| **31337** | audio track | subject |
| **36787** | music track | title, artist + album, content |
| **34139** | music playlist | title, description, content |
| **54 / 10154** | podcast episode / show | title, description, content |
| **30617** | git repository | name, description, content |
| **1621 / 1618** | git issue / pull request | subject, content |
| **1337** | code snippet | name, description, content |
| **30817** | NIP-on-Nostr document | title, content |
| **30311 / 1313** | live event / clip | title, summary, content |
| **30296 / 30297** | interactive story prologue / scene | title, summary, content |
| **31924 / 31922 / 31923** | calendar & slots | title, summary, content |
| **30312 / 30313** | meeting space / room | room or title, summary, content |
| **34550** | community | name, description + rules, content |
| **39000** | group | name, about |
| **40 / 41** | public chat channel | name, about |
| **31990** | app handler | name + display name, about |
| **32267** | software application | name, summary, content |
| **15128 / 35128** | website | title, description |
| **15129 / 35129 / 5129** | napplet root / named / snapshot | title, description |
| **30009** | badge | name, description, content |
| **30030** | emoji pack | title, description, content |
| **9041** | zap goal | summary, content |
| **33863** | fundraiser | title, content |
| **30000 / 39089** | people list / follow pack | title, description |
| **31890** | feed definition | title |
| **10003 / 30001 / 30003** | bookmark lists | title, description |
| **30015** | interest set | title, description + hashtags |
| **30004 / 30005 / 30006 / 30063 / 30267** | article / video / picture / release / app curation sets | title, description |
| **30002 / 39092 / 39701** | relay set / media starter pack / web bookmark | title, description |

Every remaining kind Quartz can parse is still indexed, by its full text
content. Plain-content kinds that carry no title — **kind 9** relay chats
(NIP-C7), comments, patches, statuses, zaps — land their whole
`indexableContent()` in the body tier, with no primary/secondary split. So every
kind implementing Quartz's `SearchableEvent` is searchable; the table above only
adds field-priority structure on top. The authoritative mapping is
[`store/…/SearchExtractors.kt`](store/src/main/kotlin/com/vitorpamplona/quartz/eventstore/store/SearchExtractors.kt).

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
}
```

For a commit snapshot, JitPack works:
`com.github.vitorpamplona.vespa-eventstore:store:<commit>`.

## Two things to know

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
