plugins {
    kotlin("jvm")
    `java-library`
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
