// The Vespa container bundle shipped inside the application package (engine's vespa-app.zip,
// under components/): request handlers that run IN Vespa's container. Plain Java, so the bundle
// carries no Kotlin runtime into an OSGi container that does not provide one.
plugins {
    java
}

dependencies {
    // Provided by the container at runtime; never packed into the bundle.
    compileOnly(libs.vespa.container.dev)
}

java {
    toolchain { languageVersion.set(JavaLanguageVersion.of(21)) }
}

// Vespa 8's container runs Java 17.
tasks.withType<JavaCompile>().configureEach { options.release.set(17) }

/**
 * What the bundle's classes reference outside java.*, from `jdeps -verbose:package` over the
 * compiled classes. A package missing here fails at deploy ("Unresolved requirement"), not at
 * build, so keep it in step with the imports of src/main/java.
 */
val containerImports =
    listOf(
        "com.fasterxml.jackson.core",
        "com.yahoo.container.jdisc",
        "com.yahoo.document",
        "com.yahoo.document.json",
        "com.yahoo.documentapi",
        "com.yahoo.jdisc",
        "com.yahoo.jdisc.http",
        "com.yahoo.messagebus",
    ).joinToString(",")

tasks.jar {
    archiveFileName.set("eventstore-bundle.jar")
    manifest {
        attributes(
            "Bundle-ManifestVersion" to "2",
            "Bundle-SymbolicName" to "eventstore-bundle",
            "Bundle-Name" to "vespa-eventstore container bundle",
            "Bundle-Version" to "1.0.0",
            // Unversioned on purpose: the bundle uses stable public container API only, and an
            // unversioned import resolves against whatever Vespa 8 container it is deployed into.
            "Import-Package" to containerImports,
        )
    }
}
