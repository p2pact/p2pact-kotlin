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
- `setRelay(RelayHint(addr, fingerprint))` — our relay, for pairing and for the pair's channel.
- `dictState(peer)` tells which dictionary compresses what we send; `noLearning` turns
  training off, `noCompression` compression.
- `befriend` adds a device known from the family book, without pairing; `forget` drops one.

All protocol state lives on one thread; roads receive on their own threads, training and
file writes run on threads of their own, so sending never waits for them.

## Status

Identity and keys, invites, known devices (`net-key.json`, `net-peers.json` in p2pact-go's
format, replaced atomically), pairing (`Noise_XXpsk2`), sessions (`Noise_IK` with data in the
first packet, replay protection, restarts), UDP, TCP and relay roads, delivery (parts, ack
ranges, streams, congestion window, RACK on unreliable roads only), CBOR calls, results, errors
and events, compression with the common dictionary and pair-scoped ones (stable slots,
proactive fetch, a missing dictionary held and fetched), own dictionaries learnt off the
sending path and trained in a lull.

Own dictionaries are trained by zstd's own trainer (`ZDICT_trainFromBuffer`, fastCover — the
algorithm of `zstd --train`), which writes the standard dictionary format the spec requires.

Not yet: key renewal every 2 hours, short names (`0.4 alias`), forwarding via a third device,
the family tool, Bluetooth.

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

## License

[Apache License 2.0](LICENSE).
