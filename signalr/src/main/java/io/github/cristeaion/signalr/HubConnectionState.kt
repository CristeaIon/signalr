package io.github.cristeaion.signalr

enum class HubConnectionState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,

    /**
     * The connection dropped unexpectedly and automatic reconnect is trying to restore it. Only
     * reached when [HubConnectionBuilder.withAutomaticReconnect] is configured. `send` and
     * `invoke` throw in this state.
     */
    RECONNECTING,
}
