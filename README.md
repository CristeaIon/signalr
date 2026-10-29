# SignalR Kotlin Client

Lightweight SignalR Client library for Android and Kotlin.

## Installation

The library is published to Maven Central. Make sure `mavenCentral()` is among your repositories, then add the dependency in your module's `build.gradle.kts`:

```kotlin
dependencies {
    implementation("io.github.cristeaion:signalr:<version>")
}
```

## Publishing to Maven Central

Publishing uses the [vanniktech maven-publish plugin](https://vanniktech.github.io/gradle-maven-publish-plugin/central/) and uploads to the Sonatype Central Portal. You need:

- A Central Portal account with the `io.github.cristeaion` namespace verified, and a user token generated from it.
- A GPG key for signing, with its public key uploaded to a keyserver (e.g. `keyserver.ubuntu.com`).

Put the credentials in `~/.gradle/gradle.properties` (never in the repo):

```properties
mavenCentralUsername=<central-portal-token-username>
mavenCentralPassword=<central-portal-token-password>

signing.keyId=<last-8-chars-of-key-id>
signing.password=<key-passphrase>
signing.secretKeyRingFile=/Users/<you>/.gnupg/secring.gpg
```

On CI, use environment variables instead: `ORG_GRADLE_PROJECT_mavenCentralUsername`, `ORG_GRADLE_PROJECT_mavenCentralPassword`, `ORG_GRADLE_PROJECT_signingInMemoryKey` (ASCII-armored private key), `ORG_GRADLE_PROJECT_signingInMemoryKeyId` and `ORG_GRADLE_PROJECT_signingInMemoryKeyPassword`.

Then publish:

```bash
# Upload and release automatically once validation passes
./gradlew :signalr:publishAndReleaseToMavenCentral -PVERSION_NAME=1.0.0

# Or upload only, then release manually from https://central.sonatype.com/publishing
./gradlew :signalr:publishToMavenCentral -PVERSION_NAME=1.0.0
```

Try it locally first with `./gradlew :signalr:publishToMavenLocal -PVERSION_NAME=1.0.0-SNAPSHOT`.

## Run the example app offline

The example app talks to a local mock server instead of a real backend. The mock provides a demo login and a classic SignalR 2.x chat hub (`chathub`).

1. Start the mock server (port 8080 by default; pass `--args="--port 9090"` to change it):

   ```bash
   ./gradlew :mockserver:run
   ```

2. Forward the port to your emulator or USB device (once per device):

   ```bash
   adb reverse tcp:8080 tcp:8080
   ```

3. Run the `app` module. Tap **Login & Extract SID** with the pre-filled demo user, then **Connect**, and send a chat message. The `ChatBot` replies to every message.

Demo users: `sa@example.com`, `alice@example.com`, `bob@example.com`, all with password `111111`.

Hub methods: `Send(message)`, `GetHistory()`, `GetOnlineUsers()`. Server events: `ReceiveMessage(user, message)`, `UserJoined(user)`, `UserLeft(user)`.

Failure switches, for trying out error handling:

```bash
curl -X POST http://127.0.0.1:8080/mock/control/reject-next-negotiate
curl -X POST http://127.0.0.1:8080/mock/control/fail-next-invoke
curl -X POST http://127.0.0.1:8080/mock/control/drop-all
curl -X POST "http://127.0.0.1:8080/mock/control/init-delay?ms=20000"
curl -X POST http://127.0.0.1:8080/mock/control/reset
```

## Tests

- **JVM** (no device): `./gradlew :app:testDebugUnitTest :mockserver:test :signalr:testDebugUnitTest`. `MockServerEndToEndTest` drives the real client and auth service over real sockets against an in-process mock server.
- **On device** (emulator or phone): `./gradlew :app:connectedDebugAndroidTest`. `ChatFlowTest` drives the app's UI (login, chat, a second user, invokes, failure switches) against a mock server started inside the test on the device. No `./gradlew :mockserver:run` and no `adb reverse` needed. With a phone plugged in, set `ANDROID_SERIAL` to choose the device.
