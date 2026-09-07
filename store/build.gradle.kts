plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.vanniktech.mavenPublish)
}

dependencies {
    // The store implements Quartz's IEventStore on top of :engine's
    // port — both appear in its public API.
    api(libs.quartz)
    api(project(":engine"))
    implementation(libs.kotlinx.coroutines)
    testImplementation(kotlin("test"))
    testImplementation(testFixtures(project(":engine")))
    // Virtual-time test clock: measures read/write serialization deterministically
    // by injecting per-round-trip delays into the index (see BatchIngestConcurrencyTest).
    testImplementation(libs.kotlinx.coroutines.test)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()

    // ModuleBoundariesTest and PortDecoratorsTest assert the SHAPE of the
    // source tree — packages, layers, which decorator overrides what — by
    // reading `*.kt` files rather than classes. Gradle cannot see that: with
    // only this module's classes as inputs, a violation added in `:engine`
    // (or a test filed into the wrong package there) leaves this task
    // UP-TO-DATE and the guard silently unrun. Naming the two trees as inputs
    // makes them part of the task's fingerprint, so the guards run when the
    // thing they guard changes. CI is a fresh checkout and always ran them;
    // this is for the machine where the mistake is actually made.
    inputs
        .files(
            fileTree(rootDir.resolve("engine/src")) { include("**/*.kt") },
            fileTree(rootDir.resolve("store/src")) { include("**/*.kt") },
        ).withPropertyName("sourceTreeUnderGuard")
        .withPathSensitivity(PathSensitivity.RELATIVE)
}

mavenPublishing {
    coordinates(
        groupId = "com.nosfabrica.vespa.eventstore",
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
