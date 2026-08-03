plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.vanniktech.mavenPublish)
}

dependencies {
    // The store implements Quartz's IEventStore on top of :vespa's engine
    // port — both appear in its public API.
    api(libs.quartz)
    api(project(":vespa"))
    implementation(libs.kotlinx.coroutines)
    testImplementation(kotlin("test"))
    testImplementation(testFixtures(project(":vespa")))
    // Virtual-time test clock: measures read/write serialization deterministically
    // by injecting per-round-trip delays into the index (see BatchIngestConcurrencyTest).
    testImplementation(libs.kotlinx.coroutines.test)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}

mavenPublishing {
    coordinates(
        groupId = "com.vitorpamplona.quartz.eventstore",
        artifactId = "store",
        version = libs.versions.app.get(),
    )
    publishToMavenCentral(automaticRelease = true)
    signAllPublications()
    pom {
        name = "Vespa Event Store"
        description = "A Vespa-backed Quartz IEventStore with trust-ranked NIP-50 search: the open() front door, Nostr storage semantics, and the NIP-85 trust projection."
        inceptionYear = "2026"
        url = "https://github.com/NosFabrica/vespa-eventstore/"
        licenses {
            license {
                name = "MIT License"
                url = "https://github.com/NosFabrica/vespa-eventstore/blob/main/LICENSE"
            }
        }
        developers {
            developer {
                id = "nosfabrica"
                name = "NosFabrica"
                url = "https://nosfabrica.com"
            }
        }
        scm {
            url = "https://github.com/NosFabrica/vespa-eventstore/"
            connection = "https://github.com/NosFabrica/vespa-eventstore/.git"
        }
    }
}
