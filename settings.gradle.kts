rootProject.name = "p2pact-kotlin"

// The root project is the protocol library (io.github.p2pact:p2pact-kotlin), as before: an
// includeBuild of this repository (the Android app) still resolves it by these coordinates.
include(":client", ":examples:cli")
project(":client").name = "p2pact-client" // io.github.p2pact:p2pact-client
project(":examples:cli").name = "p2pact-cli"
