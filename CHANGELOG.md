# Changelog

All notable changes to the `io.github.cristeaion:signalr` library are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/). While the version is `0.x`, a minor or patch
release may contain breaking changes; they are always listed under **Breaking**.

## [Unreleased]

### Breaking

- `HubConnectionState` has a new value, `RECONNECTING`. An exhaustive `when` over the state
  must handle it.
- `HubConnection` has new members: `reconnectedFlow`, and a `timeoutMs` parameter on
  `invoke()`. Existing calls still compile, but code compiled against 0.0.2 must be rebuilt,
  and custom implementations of the interface must add them.
- `invoke()` calls waiting for a result when the connection drops, or when `disconnect()` /
  `close()` runs, now fail with `ConnectionLostException`. They used to be cancelled, which
  ended the calling coroutine silently.
- `on(...)` handlers no longer run on the socket reader. They run one at a time, in arrival
  order, on a background thread of their own. As a result, an `invoke()` result can now reach
  its caller before the handler for an event the server sent earlier has run.
- The default `User-Agent` is now `signalr-kotlin/<version>`. It used to be a browser-like
  string naming a company. If your server or firewall checks it, set your own with
  `withHeader("User-Agent", ...)`.
- The default log level is `WARN`; it used to be `DEBUG`. `ConsoleSignalRLogger()` also
  defaults to `WARN`. Call `withLogLevel(...)` to see connection steps.
- The library no longer depends on `androidx.appcompat`, `androidx.core-ktx` or
  `com.google.android.material`. Apps that got them through this library, for example for a
  `Theme.MaterialComponents` theme, must now declare them themselves.
- An argument with no serializer now makes `send()` / `invoke()` throw
  `IllegalArgumentException`. It used to be sent as `toString()`.
- `connect()` called while a connect or reconnect is in progress now waits for that attempt.
  It returns when the attempt connects, or throws why it didn't. It used to return immediately
  while the state was still `CONNECTING`.

### Added

- Automatic reconnect, off by default: `withAutomaticReconnect()` (exponential backoff with
  jitter, about 1s, 2s, 4s, 8s, 16s, then every 30s, unlimited attempts),
  `withAutomaticReconnect(listOf(...))` (fixed delays, then give up), or
  `withAutomaticReconnect(policy)` with your own `RetryPolicy`. It starts after a socket error,
  a socket closed by the server, a server timeout, or a close message that allows reconnect.
  A failed first `connect()` is not retried.
- `RetryPolicy`, `RetryContext` and `ExponentialBackoffRetryPolicy` (initial and maximum
  delay, multiplier, jitter, `maxRetries`, `maxElapsedMs`).
- `ReconnectFailedException`, emitted on `errorFlow` when a retry policy gives up.
- `reconnectedFlow`, and the `onReconnected(scope) { }` extension, to restore groups and
  subscriptions after a reconnect.
- Per-call `invoke()` timeout (`invoke("Get", id, timeoutMs = 5_000)`) and a default for all
  calls, set with `withInvocationTimeout(ms)`. When time runs out, `invoke()` throws
  `InvocationTimeoutException` and the connection stays up.
- `ConnectionLostException`. Its `cause` says why the connection dropped, when that's known.
- Classic SignalR: `disconnect()` and `close()` send `/abort` (best effort, up to 2s), so the
  server runs `OnDisconnected` at once instead of after its DisconnectTimeout. The server's
  disconnect command (`"D":1`) ends the connection without reconnecting, and its reconnect
  command (`"T":1`) drops the connection so automatic reconnect can restore it.
- ASP.NET Core close messages (type 7) are now handled: the client disconnects, and reconnects
  only if the server sends `allowReconnect: true`. If the message has an error, it is emitted
  on `errorFlow`.
- Keep-alive for ASP.NET Core servers. The client sends a ping after the keep-alive interval
  with nothing sent (default 15s, `withKeepAliveInterval`). If nothing arrives for the server
  timeout (default 30s, `withServerTimeout`), it closes the connection and emits
  `ServerTimeoutException`. Classic servers use the `KeepAliveTimeout` they announce during
  negotiate.
- `HubConnection` is `AutoCloseable`. `close()` disconnects without suspending, releases the
  HTTP client, and makes later `connect()` calls throw.
- `SignalRLogger.isLoggable(level)` and a lazy `log(level, tag) { message }` overload. All
  library logging is lazy, so a disabled level costs nothing.

### Changed

- Tokens, session ids, URL query strings, hub-call arguments and server payloads are logged
  only at `VERBOSE`. `DEBUG` and above show method names and ids, never content.
- The library no longer answers server pings with a reply that was missing its record
  separator.

### Fixed

- A failed `connect()` (token provider, negotiate, handshake error or timeout, classic
  `/start`) closes the WebSocket it opened and returns to `DISCONNECTED`, with a single
  `errorFlow` emission.
- A reader or timer left over from an earlier connection can no longer tear down a newer one.
- A slow or blocking handler no longer delays `invoke()` results or pings, and can't cause a
  false server timeout.
- An `invoke()` started at the moment the connection dropped could wait forever. It now fails
  with `ConnectionLostException`.

## [0.0.2] - 2026-11-06

### Changed

- The library source is split into one file per class, model, logger, and protocol helper.
  The public API is unchanged.
- The README covers client usage in detail.

### Added

- CI workflow (unit tests and release AAR build) and an automated release workflow (Maven
  Central publishing, git tag, GitHub release).

## [0.0.1] - 2026-11-02

### Added

- First release: a SignalR client for Android and Kotlin over WebSockets with the JSON
  protocol, for ASP.NET Core SignalR and classic ASP.NET SignalR 2.x servers.
- `HubConnectionBuilder` with headers, access token provider, logger and log level, handshake
  timeout, and an option to skip negotiate.
- `connect()`, `disconnect()`, `send()`, `invoke()` and typed `invoke<T>()`.
- `on(...)` handlers with typed overloads for up to three arguments, `onFlow(...)`,
  `eventsFlow`, `errorFlow` and `connectionState`.
- Classic SignalR 2.x support: hubs declared in `connectionData`, `hub.method` names, and
  ready-made `{"H":..,"M":..,"A":..}` envelopes.
- Example app and an offline mock server with a demo login and a chat hub.
- Released under the MIT license.

[Unreleased]: https://github.com/CristeaIon/signalr/compare/0.0.2...HEAD
[0.0.2]: https://github.com/CristeaIon/signalr/compare/0.0.1...0.0.2
[0.0.1]: https://github.com/CristeaIon/signalr/releases/tag/0.0.1
