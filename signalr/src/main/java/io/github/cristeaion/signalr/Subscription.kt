package io.github.cristeaion.signalr

/** Handle returned by [HubConnection.on]; cancelling it unregisters that one handler. */
fun interface Subscription {
    fun cancel()
}
