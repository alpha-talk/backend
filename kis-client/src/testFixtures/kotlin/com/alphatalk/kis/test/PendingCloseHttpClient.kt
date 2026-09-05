package com.alphatalk.kis.test

import java.net.Authenticator
import java.net.CookieHandler
import java.net.ProxySelector
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.WebSocket
import java.nio.ByteBuffer
import java.time.Duration
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.Executor
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLParameters

class PendingCloseHttpClient(
    private val delegate: HttpClient = HttpClient.newHttpClient(),
) : HttpClient() {
    override fun cookieHandler(): Optional<CookieHandler> = delegate.cookieHandler()
    override fun connectTimeout(): Optional<Duration> = delegate.connectTimeout()
    override fun followRedirects(): Redirect = delegate.followRedirects()
    override fun proxy(): Optional<ProxySelector> = delegate.proxy()
    override fun sslContext(): SSLContext = delegate.sslContext()
    override fun sslParameters(): SSLParameters = delegate.sslParameters()
    override fun authenticator(): Optional<Authenticator> = delegate.authenticator()
    override fun version(): Version = delegate.version()
    override fun executor(): Optional<Executor> = delegate.executor()

    override fun <T> send(request: HttpRequest, handler: HttpResponse.BodyHandler<T>): HttpResponse<T> =
        delegate.send(request, handler)

    override fun <T> sendAsync(request: HttpRequest, handler: HttpResponse.BodyHandler<T>): CompletableFuture<HttpResponse<T>> =
        delegate.sendAsync(request, handler)

    override fun <T> sendAsync(
        request: HttpRequest,
        handler: HttpResponse.BodyHandler<T>,
        pushPromiseHandler: HttpResponse.PushPromiseHandler<T>?,
    ): CompletableFuture<HttpResponse<T>> = delegate.sendAsync(request, handler, pushPromiseHandler)

    override fun newWebSocketBuilder(): WebSocket.Builder = PendingCloseBuilder(delegate.newWebSocketBuilder())

    private class PendingCloseBuilder(private val delegate: WebSocket.Builder) : WebSocket.Builder {
        override fun header(name: String, value: String): WebSocket.Builder = apply { delegate.header(name, value) }
        override fun connectTimeout(timeout: Duration): WebSocket.Builder = apply { delegate.connectTimeout(timeout) }
        override fun subprotocols(mostPreferred: String, vararg lesserPreferred: String): WebSocket.Builder =
            apply { delegate.subprotocols(mostPreferred, *lesserPreferred) }

        override fun buildAsync(uri: URI, listener: WebSocket.Listener): CompletableFuture<WebSocket> =
            delegate.buildAsync(uri, listener).thenApply { PendingCloseWebSocket(it) }
    }

    private class PendingCloseWebSocket(private val delegate: WebSocket) : WebSocket {
        override fun sendText(data: CharSequence, last: Boolean): CompletableFuture<WebSocket> = delegate.sendText(data, last)
        override fun sendBinary(data: ByteBuffer, last: Boolean): CompletableFuture<WebSocket> = delegate.sendBinary(data, last)
        override fun sendPing(message: ByteBuffer): CompletableFuture<WebSocket> = delegate.sendPing(message)
        override fun sendPong(message: ByteBuffer): CompletableFuture<WebSocket> = delegate.sendPong(message)
        override fun sendClose(statusCode: Int, reason: String): CompletableFuture<WebSocket> = CompletableFuture()
        override fun request(n: Long) = delegate.request(n)
        override fun getSubprotocol(): String = delegate.subprotocol
        override fun isOutputClosed(): Boolean = delegate.isOutputClosed
        override fun isInputClosed(): Boolean = delegate.isInputClosed
        override fun abort() = delegate.abort()
    }
}
