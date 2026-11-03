# p2pact-kotlin

Kotlin implementation of **P2PACT — the P2P Agent Chat Transport Protocol** for the JVM and
Android: chat with your AI agents, and let them chat with each other, across your own devices —
no servers, no accounts, over any connection.

Spec: [p2pact/p2pact](https://github.com/p2pact/p2pact). It interoperates with the
[Go](https://github.com/p2pact/p2pact-go), [TypeScript](https://github.com/p2pact/p2pact-ts)
and [Python](https://github.com/p2pact/p2pact-py) implementations: pairing, calls both ways on
streams, long and compressed messages, own dictionaries — over UDP and TCP, checked by the interop
tests in [p2pact-tests](https://github.com/p2pact/p2pact-tests); the relay road against
p2pact-go's relay server.

A Kotlin/JVM library (JVM 17 bytecode), usable on Android API 26+. Dependencies:

- [Bouncy Castle](https://www.bouncycastle.org/) `bcprov-jdk18on`, its lightweight API only
  (X25519, Ed25519, ChaCha20-Poly1305, BLAKE2s): the same on every JVM and on Android, no JCA
  provider to register.
- [zstd-jni](https://github.com/luben/zstd-jni): zstd with dictionaries and zstd's own trainer.
  On Android use the `@aar` artifact, which carries the native libraries for Android ABIs.

Noise, CBOR and the small JSON reader for the device files are implemented here.

## Install

```kotlin
dependencies {
    implementation("io.github.p2pact:p2pact-kotlin:0.1.0") // the protocol
    implementation("io.github.p2pact:p2pact-client:0.1.0")  // the client SDK (brings the protocol)
}
```

From Maven Central (`mavenCentral()`). On Android also add `com.github.luben:zstd-jni:1.5.7-6@aar`.

## Use

```kotlin
import io.github.p2pact.*
import java.io.File

val laptop = Node(File("laptop-data"), "Laptop")      // keys and known devices live here
laptop.listenUdp("0.0.0.0:48730")
laptop.listenTcp("0.0.0.0:48730")                       // fallback where UDP is blocked
laptop.handle(1, 2) { peer, args -> emptyList() }       // chat.send: accept a message
laptop.addDict(CommonDict.raw)                          // the common dictionary (spec §5)
laptop.start()

val invite = laptop.invite(10 * 60_000).toString()      // p2pact1.… — show it as a QR code

val phone = Node(File(context.filesDir, "p2pact"), "Phone")
phone.listenUdp("0.0.0.0:0")
phone.listenTcp("")                                     // dial only: behind a carrier NAT
phone.start()
val peer = phone.join(Invite.parse(invite))             // pair once
phone.callOn(peer.addr, 1, 1, 2, 1, "run the tests")    // stream 1: one per chat
    .thenAccept { result -> println(result) }
```

- `call` / `callOn` return a `CompletableFuture` with the result elements (decoded CBOR:
  `Long`, `ByteArray`, `String`, `List`, `Map`, `Boolean`, `Double`, `null`); an error answer
  completes it with `P2pactError(code, text)`. With kotlinx-coroutines, `.await()` it.
- `handle` registers a method; handlers run on a worker pool and may block.
- `event` / `eventOn` send events; `onEvent` receives them on the worker pool.
- Calls and events of one device on one stream run one at a time, in the order sent; other
  streams do not wait for them (a slow handler holds up its own stream only). Service calls
  (tool 0) run at once.
- A handler that waits long (an agent's turn, a person's answer) returns `later(future)`: its
  stream goes on at once, and the call's result goes when the future completes.
- `setRelay(RelayHint(addr, fingerprint))` — our relay, for pairing and for the pair's channel.
- `setPaused(true)` before the computer sleeps, `setPaused(false)` when it wakes: while paused
  the node sends nothing (relay and TCP connections closed, no keepalives), so it does not wake
  the computer; messages wait in their queues, calls keep their timeouts. On resume it speaks to
  its devices first and sessions come back at once.
- `dictState(peer)` tells which dictionary compresses what we send; `noLearning` turns
  training off, `noCompression` compression.
- `befriend` adds a device known from the family book, without pairing; `forget` drops one.
- `attachRoad` — a road your app provides: Bluetooth LE GATT, a serial line, a pipe (spec §3).
  The app implements `AppRoad.send` and hands what arrives to `receive`:

  ```kotlin
  // GATT: ordered and acknowledged, writes of ATT MTU − 3 bytes — a stream road
  val ble = node.attachRoad(peerAddr, AppRoad { chunk -> gattWrite(chunk) },  // may block until written
      AppRoadConfig("ble", stream = true, mtu = mtu - 3, rate = 40 shl 10))
  // in onCharacteristicWriteRequest / onCharacteristicChanged:
  ble.receive(value)
  // the link dropped:
  ble.detach()
  ```

  `peerAddr` is the device the link reaches if the app knows it (from its advertisement); `null`
  — learnt from the first handshake on it, and offered to `join` for pairing. The road is used
  while nothing better is up; the session moves to UDP, TCP or a relay as soon as one answers
  and comes back when they fall silent.

All protocol state lives on one thread; roads receive on their own threads, training and
file writes run on threads of their own, so sending never waits for them.

## Client SDK (`p2pact-client`)

`client/` (`io.github.p2pact:p2pact-client`, package `io.github.p2pact.client`) is a typed client
of the public tools of the P2PACT engine: `p2pact.chat` (ACP, tool 1), `p2pact.fleet` (tool 2)
and `p2pact.plugins` (tool 4). Pure Kotlin/JVM on top of this library; calls return
`CompletableFuture`, events go to `Events<T>` listeners.

```kotlin
val client = P2pactClient(File(dataDir), "My app")
val computer = client.pair("p2pact1.…").await()        // asks for chat, fleet, plugins:read
computer.chat.listAllSessions().await()                 // initialize goes first by itself
computer.chat.updates.subscribe { u -> println(u) }
computer.chat.prompt("session-id", "run the tests").await()
```

Errors are `P2pactException` (`ForbiddenException` is error 4, …) and, for the tool's own
errors, `RpcException`. See `examples/cli` for a small program.

## Status

Identity and keys, invites, known devices (`net-key.json`, `net-peers.json` in p2pact-go's
format, replaced atomically), pairing (`Noise_XXpsk2`), sessions (`Noise_IK` with data in the
first packet, replay protection, restarts), UDP, TCP and relay roads, roads the app provides
(datagram or stream: Bluetooth LE), delivery (parts, ack ranges, streams, congestion window,
RACK on unreliable roads only), CBOR calls, results, errors and events, compression with the
common dictionary and pair-scoped ones (stable slots, proactive fetch, a missing dictionary held
and fetched), own dictionaries learnt off the sending path and trained in a lull.

Own dictionaries are trained by zstd's own trainer (`ZDICT_trainFromBuffer`, fastCover — the
algorithm of `zstd --train`), which writes the standard dictionary format the spec requires.

Not yet: key renewal every 2 hours, short names (`0.4 alias`), forwarding via a third device,
the family tool.

## Develop

```sh
./gradlew test          # JDK 17
./gradlew installDist   # build/install/p2pact-interop/bin/p2pact-interop
```

`build/install/p2pact-interop/bin/p2pact-interop` is the interop peer used by p2pact-tests:

```sh
cd ../p2pact-tests
P2PACT_KOTLIN=../p2pact-kotlin/build/install/p2pact-interop/bin/p2pact-interop go test -count=1 -v ./interop/
```

The relay test runs against p2pact-go's relay (`cmd/p2pact-relay`):

```sh
P2PACT_RELAY="127.0.0.1:8443 <fingerprint>" ./gradlew test --tests '*RelayTest'
```

## Publishing

For maintainers. `.github/workflows/publish.yml` runs on a `v*` tag (or by hand): tests, then
`./gradlew publishToMavenCentral` for both artifacts (sources and javadoc jars, signed, released
automatically). Before the first tag:

- Central Portal (central.sonatype.com): sign in with GitHub, register the namespace
  `io.github.p2pact` and verify it (GitHub organisation `p2pact`), then Account, Generate User
  Token. Repository secrets `MAVEN_CENTRAL_USERNAME` and `MAVEN_CENTRAL_PASSWORD` are the token's
  username and password.
- GPG: `gpg --full-generate-key`, publish the public key (`gpg --keyserver keyserver.ubuntu.com
  --send-keys <id>`), then `gpg --armor --export-secret-keys <id>` goes to the secret `SIGNING_KEY`
  and its passphrase to `SIGNING_PASSWORD`.
- The tag must equal `v` + `version` in `build.gradle.kts`: `git tag v0.1.0 && git push origin v0.1.0`.

Locally: `./gradlew publishToMavenLocal` (JDK 17) builds and installs unsigned artifacts into `~/.m2`.

## License

[Apache License 2.0](LICENSE).
