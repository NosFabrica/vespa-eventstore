import com.diffplug.gradle.spotless.SpotlessExtensionPredeclare
import java.io.File

plugins {
    // Load the Kotlin plugins once, on the root classpath (`apply false`): subprojects
    // applying them per-module would otherwise each get their own classloader copy
    // ("The Kotlin Gradle plugin was loaded multiple times in different subprojects").
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    alias(libs.plugins.diffplug.spotless)
    // Load once on the root classpath; the publishable modules apply it (see :engine/:store).
    alias(libs.plugins.vanniktech.mavenPublish) apply false
}

val ktlintVersion = libs.versions.ktlint.get()

allprojects {
    apply(plugin = "com.diffplug.spotless")

    if (project === rootProject) {
        // Predeclare formatter dependencies once at the root (Spotless multi-project best practice).
        spotless { predeclareDeps() }
        configure<SpotlessExtensionPredeclare> {
            kotlin { ktlint(ktlintVersion) }
            kotlinGradle { ktlint(ktlintVersion) }
        }
    } else {
        spotless {
            kotlin {
                target("src/**/*.kt")
                ktlint(ktlintVersion)
                licenseHeaderFile(
                    rootProject.file(".spotless/copyright.kt"),
                    "@file:|package|import|class|object|sealed|open|interface|abstract ",
                )
            }
            kotlinGradle {
                target("*.gradle.kts")
                ktlint(ktlintVersion)
            }
        }
    }
}

// Install the repo's git hooks (.git-hooks -> .git/hooks) so spotless runs on
// commit and tests run on push, mirroring Amethyst. Handles the worktree layout
// (where .git is a file pointing at the real gitdir) and marks the hooks
// executable on copy.
val installGitHook =
    tasks.register<Copy>("installGitHook") {
        val dotGit = File(rootProject.rootDir, ".git")
        val hooksDir: File =
            if (dotGit.isFile) {
                val gitDir = File(dotGit.readText().trim().replace("gitdir: ", ""))
                File(gitDir, "hooks")
            } else {
                File(dotGit, "hooks")
            }
        from(File(rootProject.rootDir, ".git-hooks/pre-commit"))
        from(File(rootProject.rootDir, ".git-hooks/pre-push"))
        into(hooksDir)
        filePermissions { unix("0777") }
    }

// Run before compilation so a plain `./gradlew build` installs the hooks.
subprojects {
    tasks.matching { it.name == "compileKotlin" }.configureEach {
        dependsOn(installGitHook)
    }
}
