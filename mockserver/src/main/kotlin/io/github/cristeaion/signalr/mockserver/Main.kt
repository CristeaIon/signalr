package io.github.cristeaion.signalr.mockserver

import java.io.IOException
import java.net.InetSocketAddress
import java.net.ServerSocket
import kotlin.system.exitProcess

private const val DEFAULT_PORT = 8080

fun main(args: Array<String>) {
    val port = args.indexOf("--port").takeIf { it >= 0 }
        ?.let { args.getOrNull(it + 1)?.toIntOrNull() }
        ?: DEFAULT_PORT

    // Ktor reports a taken port as an uncaught coroutine failure, so probe it first.
    try {
        ServerSocket().use { it.bind(InetSocketAddress("127.0.0.1", port)) }
    } catch (e: IOException) {
        System.err.println("Port $port is already in use. Pass --port <n> to pick another one.")
        exitProcess(1)
    }

    val server = MockServer()
    server.start(port)
    Runtime.getRuntime().addShutdownHook(Thread { server.stop() })

    val base = "http://127.0.0.1:$port"
    println(
        """
        |=== SignalR mock server (TEST FIXTURE - do not expose) ===
        |Listening on $base
        |
        |Login URL:   $base/api/login
        |SignalR URL: $base/signalr/negotiate?connectionData=[{"name":"$CHAT_HUB"}]&sessionId=<sid from login>
        |Demo users:  ${DEMO_USERS.entries.joinToString { "${it.key} / ${it.value}" }}
        |
        |Android emulator or USB device: run `adb reverse tcp:$port tcp:$port` once per device.
        |Failure switches: curl -X POST $base/mock/control/{reject-next-negotiate|fail-next-invoke|drop-all|init-delay?ms=N|reset}
        |Press Ctrl+C to stop.
        """.trimMargin()
    )
    Thread.currentThread().join()
}
