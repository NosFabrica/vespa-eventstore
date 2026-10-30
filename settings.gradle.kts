pluginManagement {
    repositories {
        gradlePluginPortal()
        google()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        google()
        // Quartz comes from Maven Central since 2.4.0 (libs.versions.toml), the last thing this build
        // read from JitPack. jitpack.yml stays: JitPack still BUILDS this store for vespa-relay.
        mavenCentral()
    }
}

rootProject.name = "vespa-eventstore"

// Quartz (com.vitorpamplona.quartz:quartz) comes from Maven Central, pinned in
// gradle/libs.versions.toml. To change it alongside this store — a Nostr rule
// this store needs that quartz lacks goes upstream first (AGENTS.md, "What goes
// where") — point Gradle at a local checkout and it builds that checkout in
// place of the published artifact, the way quartz's own -PcryptoKmpDir does:
//   ./gradlew -PquartzDir=../quartz build
// Every module that names libs.quartz then compiles and tests against the
// checkout's :quartz JVM variant. The published POM still names the pin, so a
// change that needs the new quartz waits for its release before this store's.
providers.gradleProperty("quartzDir").orNull?.let { dir ->
    includeBuild(dir) {
        dependencySubstitution {
            substitute(module("com.vitorpamplona.quartz:quartz")).using(project(":quartz"))
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
