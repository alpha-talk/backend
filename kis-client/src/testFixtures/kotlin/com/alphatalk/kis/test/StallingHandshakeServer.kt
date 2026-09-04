package com.alphatalk.kis.test

import java.io.InputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.thread

class StallingHandshakeServer : AutoCloseable {
    private val serverSocket = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
    private val pending = CopyOnWriteArrayList<PendingHandshake>()
    private val acceptor = thread(isDaemon = true, name = "stalling-handshake-acceptor") { acceptLoop() }

    private class PendingHandshake(val socket: Socket, val key: String)

    val url: String
        get() = "ws://127.0.0.1:${serverSocket.localPort}"

    val connectionCount: Int
        get() = pending.size

    fun awaitConnections(count: Int, timeoutMillis: Long = 5000) {
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (System.currentTimeMillis() < deadline && pending.size < count) Thread.sleep(20)
        check(pending.size >= count) { "await timed out: connections=$count actual=${pending.size}" }
    }

    fun completeHandshakes() {
        pending.forEach { handshake ->
            val accept = Base64.getEncoder().encodeToString(
                MessageDigest.getInstance("SHA-1").digest((handshake.key + WS_GUID).toByteArray()),
            )
            val response = "HTTP/1.1 101 Switching Protocols\r\n" +
                "Upgrade: websocket\r\n" +
                "Connection: Upgrade\r\n" +
                "Sec-WebSocket-Accept: $accept\r\n\r\n"
            handshake.socket.getOutputStream().apply {
                write(response.toByteArray())
                flush()
            }
        }
    }

    fun awaitPeersClosed(timeoutMillis: Long = 5000) {
        pending.forEach { handshake ->
            handshake.socket.soTimeout = timeoutMillis.toInt()
            val input = handshake.socket.getInputStream()
            val closed = runCatching {
                while (input.read() != -1) Unit
                true
            }.getOrElse { it !is SocketTimeoutException }
            check(closed) { "peer kept the connection open" }
        }
    }

    override fun close() {
        serverSocket.close()
        pending.forEach { runCatching { it.socket.close() } }
        acceptor.join(1000)
    }

    private fun acceptLoop() {
        while (!serverSocket.isClosed) {
            val socket = runCatching { serverSocket.accept() }.getOrNull() ?: return
            socket.soTimeout = HEADER_READ_TIMEOUT_MILLIS
            val key = runCatching { readHandshakeKey(socket.getInputStream()) }.getOrNull()
            if (key == null) socket.close() else pending += PendingHandshake(socket, key)
        }
    }

    private fun readHandshakeKey(input: InputStream): String? {
        val header = StringBuilder()
        while (!header.endsWith("\r\n\r\n")) {
            val byte = input.read()
            if (byte == -1) return null
            header.append(byte.toChar())
        }
        return header.lineSequence()
            .firstOrNull { it.startsWith("Sec-WebSocket-Key:", ignoreCase = true) }
            ?.substringAfter(':')
            ?.trim()
    }

    private companion object {
        const val WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        const val HEADER_READ_TIMEOUT_MILLIS = 5000
    }
}
