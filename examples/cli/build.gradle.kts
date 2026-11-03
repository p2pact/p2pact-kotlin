plugins {
    kotlin("jvm")
    application
}

group = "io.github.p2pact"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":p2pact-client"))
}

kotlin {
    jvmToolchain(17)
}

application {
    applicationName = "p2pact-cli"
    mainClass.set("io.github.p2pact.cli.MainKt")
}
