package io.github.cristeaion.signalr

/** Handle returned by [HubConnection.on]; cancelling it unregisters that one handler. */
public fun interface Subscription {
    public fun cancel()
}
