package com.alphatalk.kis.ws

import com.alphatalk.kis.KisClientException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit

class KisWebSocketSession(
    private val wsUrl: String,
    private val approvalKey: String,
    private val listener: KisSessionListener,
    private val http: HttpClient = HttpClient.newHttpClient(),
) {
    private val mapper: ObjectMapper = jacksonObjectMapper()
    private val sendLock = Any()
    private val lifecycle = Any()
    private var closed = false

    @Volatile
    private var webSocket: WebSocket? = null

    val isOpen: Boolean
        get() = webSocket?.let { !it.isInputClosed && !it.isOutputClosed } == true

    fun connect(handshakeTimeout: Duration = DEFAULT_HANDSHAKE_TIMEOUT): CompletableFuture<Void> =
        http.newWebSocketBuilder()
            .connectTimeout(handshakeTimeout)
            .buildAsync(URI.create(wsUrl), FrameListener())
            .thenAccept(::adopt)

    fun subscribe(trKey: String, trId: String = KisFrameParser.TR_ID_TICK) {
        sendRegistration("1", trId, trKey)
    }

    fun unsubscribe(trKey: String, trId: String = KisFrameParser.TR_ID_TICK) {
        sendRegistration("2", trId, trKey)
    }

    fun close() {
        val current = detach() ?: return
        try {
            current.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown").get(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        } catch (e: InterruptedException) {
            runCatching { current.abort() }
            Thread.currentThread().interrupt()
        } catch (e: Exception) {
            runCatching { current.abort() }
        }
    }

    fun abort() {
        detach()?.let { runCatching { it.abort() } }
    }

    private fun adopt(ws: WebSocket) {
        synchronized(lifecycle) {
            if (!closed) {
                webSocket = ws
                return
            }
        }
        runCatching { ws.abort() }
        throw KisClientException("session closed before handshake completed")
    }

    private fun detach(): WebSocket? = synchronized(lifecycle) {
        closed = true
        webSocket.also { webSocket = null }
    }

    private fun sendRegistration(trType: String, trId: String, trKey: String) {
        val message = mapper.writeValueAsString(
            mapOf(
                "header" to mapOf(
                    "approval_key" to approvalKey,
                    "custtype" to "P",
                    "tr_type" to trType,
                    "content-type" to "utf-8",
                ),
                "body" to mapOf("input" to mapOf("tr_id" to trId, "tr_key" to trKey)),
            ),
        )
        sendText(currentWebSocket(), message)
    }

    private fun currentWebSocket(): WebSocket =
        webSocket ?: throw KisClientException("session not connected")

    private fun sendText(target: WebSocket, text: String) {
        synchronized(sendLock) {
            target.sendText(text, true).join()
        }
    }

    private inner class FrameListener : WebSocket.Listener {
        private val partial = StringBuilder()

        override fun onOpen(ws: WebSocket) {
            ws.request(1)
            listener.onOpen()
        }

        override fun onText(ws: WebSocket, data: CharSequence, last: Boolean): CompletionStage<*>? {
            partial.append(data)
            if (last) {
                val text = partial.toString()
                partial.setLength(0)
                runCatching { handle(ws, text) }.onFailure { listener.onError(it) }
            }
            ws.request(1)
            return null
        }

        override fun onClose(ws: WebSocket, statusCode: Int, reason: String): CompletionStage<*>? {
            listener.onClosed("code=$statusCode reason=$reason")
            return null
        }

        override fun onError(ws: WebSocket, error: Throwable) {
            listener.onTransportError(error)
        }

        private fun handle(ws: WebSocket, text: String) {
            when (val frame = KisFrameParser.parse(text)) {
                is KisFrame.PingPong -> {
                    sendText(ws, text)
                    listener.onPingPong()
                }
                is KisFrame.Ticks -> listener.onTicks(frame.trId, frame.ticks)
                is KisFrame.Depths -> listener.onDepths(frame.trId, frame.depths)
                is KisFrame.Control ->
                    if (frame.unsubscribe) {
                        listener.onUnsubscribeAck(frame.trId, frame.trKey, frame.success)
                    } else {
                        listener.onSubscribeAck(frame.trId, frame.trKey, frame.success)
                    }
                is KisFrame.EncryptedDropped -> listener.onEncryptedDropped(frame.trId)
                is KisFrame.Unknown -> Unit
            }
        }
    }

    private companion object {
        const val CLOSE_TIMEOUT_SECONDS = 5L
        val DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration.ofSeconds(10)
    }
}
