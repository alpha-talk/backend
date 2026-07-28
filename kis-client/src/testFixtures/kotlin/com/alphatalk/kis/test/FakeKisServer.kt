package com.alphatalk.kis.test

import org.java_websocket.WebSocket
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer
import java.net.InetSocketAddress
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class FakeKisServer : WebSocketServer(InetSocketAddress("127.0.0.1", 0)), AutoCloseable {
    val receivedMessages = CopyOnWriteArrayList<String>()
    private val connections = CopyOnWriteArrayList<WebSocket>()
    private val started = CountDownLatch(1)

    init {
        isReuseAddr = true
    }

    override fun onStart() {
        started.countDown()
    }

    override fun onOpen(conn: WebSocket, handshake: ClientHandshake) {
        connections += conn
    }

    override fun onMessage(conn: WebSocket, message: String) {
        receivedMessages += message
    }

    override fun onClose(conn: WebSocket, code: Int, reason: String, remote: Boolean) {
        connections -= conn
    }

    override fun onError(conn: WebSocket?, ex: Exception) {
    }

    val url: String
        get() = "ws://127.0.0.1:$port"

    val connectionCount: Int
        get() = connections.size

    fun startAndAwait() {
        start()
        check(started.await(5, TimeUnit.SECONDS)) { "fake KIS server did not start" }
    }

    fun awaitConnections(count: Int, timeoutMillis: Long = 5000) {
        awaitCondition(timeoutMillis, "connections=$count") { connections.size >= count }
    }

    fun awaitMessages(count: Int, timeoutMillis: Long = 5000) {
        awaitCondition(timeoutMillis, "messages=$count") { receivedMessages.size >= count }
    }

    fun broadcastText(text: String) {
        connections.forEach { it.send(text) }
    }

    fun closeAllConnections() {
        connections.forEach { it.close(1000, "test-close") }
    }

    fun abortAllConnections() {
        connections.forEach { it.closeConnection(1006, "test-abort") }
    }

    override fun close() {
        stop(1000)
    }

    private fun awaitCondition(timeoutMillis: Long, label: String, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (System.currentTimeMillis() < deadline) {
            if (condition()) return
            Thread.sleep(20)
        }
        check(condition()) { "await timed out: $label" }
    }
}
