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
        mavenCentral()
        maven { url = uri("https://jitpack.io") }
    }
}

rootProject.name = "vespa-eventstore"

// A Vespa-backed Quartz IEventStore with trust-ranked NIP-50 search.
//   :engine — document shapes, YQL query building, the Vespa clients, the bundled app package
//   :store — Quartz IEventStore semantics on top, the trust projection, and the open() front door
//   :benchmark — head-to-head insert/query benchmark vs Quartz's SQLite store (not published)
include(":engine")
include(":store")
include(":benchmark")
