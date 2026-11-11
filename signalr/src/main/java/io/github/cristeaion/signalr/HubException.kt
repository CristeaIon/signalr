package io.github.cristeaion.signalr

/**
 * The server reported an error: a hub method called with [HubConnection.invoke] failed, or the
 * server closed the connection with an error. [serverError] is the server's own text.
 */
public class HubException(message: String, public val serverError: String) : RuntimeException(message)
