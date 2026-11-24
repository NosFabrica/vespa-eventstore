pluginManagement {
    repositories {
        gradlePluginPortal()
        google()
        mavenCentral()
    }
}

// Quartz (com.vitorpamplona.quartz) and its crypto-kmp (com.vitorpamplona.crypto),
// libschnorr256k1-kotlin (com.vitorpamplona.schnorr256k1) and negentropy-kmp
// (com.vitorpamplona.negentropy) are released on Nostr + Blossom only: Magpie serves those groups,
// trusting only the release key vitorpamplona.com names as `releases`, and gradle/magpie.lock pins
// that key and every resolved version's files, as in quartz itself. A consumer of this store needs
// the same groups through Magpie. While quartz itself is pinned to a commit, it comes from JitPack
// (below) and Magpie serves only the other three.
plugins {
    id("com.vitorpamplona.magpie") version "0.2.3"
}

magpie {
    group("com.vitorpamplona.quartz")
    group("com.vitorpamplona.crypto")
    group("com.vitorpamplona.schnorr256k1")
    group("com.vitorpamplona.negentropy")
}

dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        // Quartz is pinned to a commit JitPack builds (libs.versions.toml), and JitPack serves
        // nothing else here. jitpack.yml is separate: it lets JitPack BUILD this store for
        // vespa-relay.
        exclusiveContent {
            forRepository { maven { url = uri("https://jitpack.io") } }
            filter {
                includeGroup("com.github.vitorpamplona.quartz")
            }
        }
    }
}

rootProject.name = "vespa-eventstore"

// Quartz comes from JitPack (com.github.vitorpamplona.quartz:quartz), pinned to a commit in
// gradle/libs.versions.toml; its releases come from Nostr + Blossom through Magpie
// (com.vitorpamplona.quartz:quartz). To change it alongside this store — a Nostr rule
// this store needs that quartz lacks goes upstream first (AGENTS.md, "What goes
// where") — point Gradle at a local checkout and it builds that checkout in
// place of the published artifact, the way quartz's own -PcryptoKmpDir does:
//   ./gradlew -PquartzDir=../quartz build
// Every module that names libs.quartz then compiles and tests against the
// checkout's :quartz JVM variant. The published POM still names the pin, so a
// change that needs the new quartz waits for a quartz commit JitPack builds.
providers.gradleProperty("quartzDir").orNull?.let { dir ->
    includeBuild(dir) {
        dependencySubstitution {
            for (group in listOf("com.vitorpamplona.quartz", "com.github.vitorpamplona.quartz")) {
                substitute(module("$group:quartz")).using(project(":quartz"))
            }
        }
    }
}

// A Vespa-backed Quartz IEventStore with trust-ranked NIP-50 search.
//   :engine — document shapes, YQL query building, the Vespa clients, the bundled app package
//   :store — Quartz IEventStore semantics on top, the trust projection, and the open() front door
//   :benchmark — head-to-head insert/query benchmark vs Quartz's SQLite store (not published)
include(":bundle")
include(":engine")
include(":store")
include(":benchmark")
