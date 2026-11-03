plugins {
    kotlin("jvm")
    `java-library`
    id("com.vanniktech.maven.publish")
}

group = "io.github.p2pact"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    // the protocol library of this repository (io.github.p2pact:p2pact-kotlin)
    api(rootProject)
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

tasks.test {
    maxHeapSize = "1g"
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

mavenPublishing {
    publishToMavenCentral(automaticRelease = true)
    if (providers.gradleProperty("signingInMemoryKey").isPresent) signAllPublications()

    coordinates("io.github.p2pact", "p2pact-client", "0.1.0")

    pom {
        name.set("p2pact-client")
        description.set("Client SDK for P2PACT: pair with an engine, chat, fleet and plugins, for apps on the JVM and Android.")
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
