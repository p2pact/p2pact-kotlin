plugins {
    kotlin("jvm") version "2.2.0"
    `java-library`
    application
    id("com.vanniktech.maven.publish") version "0.34.0"
}

group = "io.github.p2pact"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    // X25519, Ed25519, ChaCha20-Poly1305, BLAKE2s: the lightweight API, no JCA provider
    api("org.bouncycastle:bcprov-jdk18on:1.79")
    // zstd with dictionaries and its trainer (Android: com.github.luben:zstd-jni:<version>@aar)
    api("com.github.luben:zstd-jni:1.5.7-6")
    testImplementation(kotlin("test-junit"))
    testImplementation("junit:junit:4.13.2")
}

kotlin {
    jvmToolchain(17)
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

application {
    applicationName = "p2pact-interop"
    mainClass.set("io.github.p2pact.interop.InteropKt")
}

tasks.test {
    maxHeapSize = "1g"
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

// Publishing to Maven Central (Central Portal). Credentials and the signing key come from Gradle
// properties or ORG_GRADLE_PROJECT_* environment variables (mavenCentralUsername, mavenCentralPassword,
// signingInMemoryKey, signingInMemoryKeyPassword); without a key the artifacts are not signed.
mavenPublishing {
    publishToMavenCentral(automaticRelease = true)
    if (providers.gradleProperty("signingInMemoryKey").isPresent) signAllPublications()

    coordinates("io.github.p2pact", "p2pact-kotlin", "0.1.0")

    pom {
        name.set("p2pact-kotlin")
        description.set("Kotlin/JVM implementation of P2PACT, the P2P Agent Chat Transport Protocol: chat with your AI agents across your own devices, no servers.")
        inceptionYear.set("2026")
        url.set("https://github.com/p2pact/p2pact-kotlin")
        licenses {
            license {
                name.set("Apache-2.0")
                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                distribution.set("repo")
            }
        }
        developers {
            developer {
                id.set("aidarbn")
                name.set("Aidar Babanov")
                url.set("https://github.com/aidarbn")
            }
        }
        scm {
            url.set("https://github.com/p2pact/p2pact-kotlin")
            connection.set("scm:git:https://github.com/p2pact/p2pact-kotlin.git")
            developerConnection.set("scm:git:ssh://git@github.com/p2pact/p2pact-kotlin.git")
        }
    }
}
