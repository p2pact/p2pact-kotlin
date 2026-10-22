plugins {
    kotlin("jvm") version "2.2.0"
    `java-library`
    application
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
