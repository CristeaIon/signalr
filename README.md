# SignalR Kotlin Client

Lightweight SignalR Client library for Android and Kotlin.

## Installation

The library is published to Maven Central. Make sure `mavenCentral()` is among your repositories, then add the dependency in your module's `build.gradle.kts`:

```kotlin
dependencies {
    implementation("io.github.cristeaion:signalr:<version>")
}
```

## Usage

The client works with both **ASP.NET Core SignalR** and **classic ASP.NET SignalR 2.x** servers, over WebSockets with the JSON protocol. It treats a URL as classic when its path contains `/signalr` or its query has a `connectionData` parameter.

### Create a connection

```kotlin
val connection = HubConnectionBuilder.create("https://example.com/chathub")
    .withAccessTokenProvider { authRepository.currentToken() } // sent as a Bearer header and as access_token
    .withHeader("X-Client", "android")
    .withLogger(AndroidSignalRLogger())                          // default: ConsoleSignalRLogger
    .withLogLevel(SignalRLogLevel.INFO)
    .withHandshakeTimeout(15_000)
    .withKeepAliveInterval(15_000)                               // ping when idle (Core servers); 0 = off
    .withServerTimeout(30_000)                                   // drop after this long without a message; 0 = off
    .build()
```

**Keep-alive.** On ASP.NET Core servers the client sends a ping whenever it hasn't sent anything for the keep-alive interval. If nothing arrives from the server within the server timeout, the client closes the connection, moves to `DISCONNECTED` and emits a `ServerTimeoutException` on `errorFlow`. Keep the server timeout at least twice the server's `KeepAliveInterval`. Classic servers send their own `{}` keep-alives and announce `KeepAliveTimeout` during negotiate. The client uses that value as its timeout and ignores `withServerTimeout`.

**Logging.** Messages are built lazily, so a level you've turned off costs nothing. Tokens, session ids, URL query strings, hub-call arguments and server payloads are logged only at `VERBOSE`; `DEBUG` and above show method names and ids, never content. A custom `SignalRLogger` that filters by level should also override `isLoggable(level)`. Your own code can use the same lazy form: `logger.log(SignalRLogLevel.DEBUG, "Tag") { "expensive $value" }`.

`connect()` calls `/negotiate` first. If the server accepts WebSockets directly and doesn't expose `/negotiate`, call `shouldSkipNegotiate(true)` on the builder.

### Connect and disconnect

```kotlin
lifecycleScope.launch {
    connection.connectionState.collect { state -> /* DISCONNECTED, CONNECTING, CONNECTED */ }
}

lifecycleScope.launch {
    try {
        connection.connect()
    } catch (e: Exception) {
        // negotiate, handshake or classic /start failed
    }
}

// later
connection.disconnect()   // can connect() again afterwards

// done with it for good, e.g. in onDestroy / onCleared
connection.close()        // disconnects and releases the HTTP client; does not suspend
```

A `HubConnection` owns an HTTP client, so call `close()` once you no longer need it. It is `AutoCloseable`. If `connect()` fails at any step (negotiate, handshake, handshake timeout, classic `/start`), the WebSocket that was opened is closed before the exception reaches you.

`connect()`, `disconnect()`, `send()` and `invoke()` are `suspend` functions and throw their failures to the caller. Errors that happen with no caller waiting arrive on `errorFlow`: a dropped socket, an error frame from the server, or an exception thrown inside a handler.

```kotlin
connection.onError(lifecycleScope) { throwable -> Log.e("Hub", "SignalR error", throwable) }
```

### Automatic reconnect

Reconnect is off by default. Turn it on and a connection that drops after it was established is restored by itself:

```kotlin
HubConnectionBuilder.create(url)
    .withAutomaticReconnect()                                   // backoff: ~1s, 2s, 4s, 8s, 16s, then every 30s, never gives up
    // .withAutomaticReconnect(listOf(0, 2_000, 10_000, 30_000)) // fixed delays, then give up (like the official clients)
    // .withAutomaticReconnect(ExponentialBackoffRetryPolicy(maxDelayMs = 60_000, maxElapsedMs = 5 * 60_000))
    // .withAutomaticReconnect { ctx -> if (ctx.previousRetryCount < 5) 3_000L else null } // your own RetryPolicy
    .build()
```

- **Triggers:** a socket error, the server closing the socket, a server timeout, or an ASP.NET Core close message with `allowReconnect: true`. A close message without it (for example after an auth failure) disconnects without retrying.
- **Not retried:** a failed first `connect()`. It throws as before.
- While retrying, the state is `RECONNECTING`, and `send()` / `invoke()` throw. `invoke()` calls still waiting when the connection drops are cancelled.
- `disconnect()` and `close()` stop a reconnect that is in progress.
- When a policy gives up, the state becomes `DISCONNECTED` and a `ReconnectFailedException` (with the attempt count and the last failure as its cause) is emitted on `errorFlow`.
- Each attempt negotiates again and asks the `accessTokenProvider` for a fresh token. Handlers registered with `on(...)` stay registered.
- The server sees a **new connection**, so groups and server-side subscriptions are gone. Restore them in `onReconnected`:

```kotlin
connection.onReconnected(scope) { connection.send("JoinGroup", "prices") }
```

The default policy never gives up, and the library doesn't know when your app is in the background. To avoid retrying (and draining the battery) while nobody is looking, tie the connection to the app's lifecycle, for example by calling `disconnect()` on `ProcessLifecycleOwner` `ON_STOP` and `connect()` on `ON_START`.

### Receive server events

Method names are matched case-insensitively. The typed overloads decode up to three arguments with kotlinx.serialization. If the server leaves an argument out or sends `null`, a nullable type receives `null` and a non-nullable primitive receives its default (`""`, `0`, `false`).

```kotlin
val sub = connection.on<String, String>("ReceiveMessage") { user, message ->
    // runs on a background thread: switch to Main before touching UI
}

connection.on<ChatMessage>("NewMessage") { msg -> }        // any @Serializable type

connection.on("UserJoined") { args: List<JsonElement> ->   // raw JSON arguments
    val user = args.decodeArg<String>(0)
}

sub.cancel() // unregister that handler
```

You can also collect events as a `Flow`:

```kotlin
connection.onFlow<String, String>("ReceiveMessage")
    .onEach { (user, message) -> }
    .launchIn(lifecycleScope)

connection.eventsFlow.collect { event -> /* every event: event.target, event.arguments */ }
```

> On classic connections, `onFlow` currently emits each push twice: once for `method` and once for `hub.method`. Use `on(...)` if that's a problem.

### Call hub methods

```kotlin
connection.send("Send", "Hello!")                          // fire-and-forget

val raw: JsonElement = connection.invoke("GetHistory")     // waits for the result
val users: List<String> = connection.invoke<List<String>>("GetOnlineUsers")
```

Arguments can be primitives, `Map`s, collections, arrays, `JsonElement`s or `@Serializable` objects.

### Classic SignalR 2.x

Declare the hubs in the URL's `connectionData` parameter:

```kotlin
val url = "https://example.com/signalr?connectionData=" +
    URLEncoder.encode("""[{"name":"chathub"}]""", "UTF-8")
val connection = HubConnectionBuilder.create(url).build()
```

- If the connection declares one hub, a bare method name is sent to it: `send("Send", "hi")`.
- If it declares several, qualify the name as `"chathub.Send"`. A bare name throws `IllegalArgumentException`.
- You can also pass a ready-made envelope, `{"H":"chathub","M":"Send","A":["hi"]}`, as the method name or as the only argument.
- Server pushes go to handlers registered under both `Method` and `hub.Method`.

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

## License

Copyright 2026 Ion Cristea

Licensed under the [MIT License](LICENSE).
