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

// A Vespa-backed Quartz IEventStore with trust-ranked NIP-50 search.
//   :engine — document shapes, YQL query building, the Vespa clients, the bundled app package
//   :store — Quartz IEventStore semantics on top, the trust projection, and the open() front door
//   :benchmark — head-to-head insert/query benchmark vs Quartz's SQLite store (not published)
include(":bundle")
include(":engine")
include(":store")
include(":benchmark")
