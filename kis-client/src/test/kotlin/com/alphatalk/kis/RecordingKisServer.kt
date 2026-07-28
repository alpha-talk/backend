package com.alphatalk.kis

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

class RecordingKisServer : AutoCloseable {
    data class Received(
        val method: String,
        val path: String,
        val query: String,
        val body: String,
        val headers: Map<String, String>,
    )

    private data class Response(val status: Int, val body: String, val delayMillis: Long)

    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
    private val executor = Executors.newFixedThreadPool(4)
    private val responders = ConcurrentHashMap<String, MutableList<Response>>()
    val received = CopyOnWriteArrayList<Received>()

    init {
        server.createContext("/") { exchange ->
            val body = exchange.requestBody.readBytes().decodeToString()
            val headers = exchange.requestHeaders.entries.associate {
                it.key.lowercase() to it.value.joinToString(",")
            }
            received += Received(
                method = exchange.requestMethod,
                path = exchange.requestURI.path,
                query = exchange.requestURI.rawQuery ?: "",
                body = body,
                headers = headers,
            )
            val queue = responders[exchange.requestURI.path]
            val response = when {
                queue == null || queue.isEmpty() -> Response(404, "{}", 0)
                queue.size > 1 -> queue.removeAt(0)
                else -> queue[0]
            }
            if (response.delayMillis > 0) {
                Thread.sleep(response.delayMillis)
            }
            val bytes = response.body.toByteArray()
            exchange.sendResponseHeaders(response.status, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        }
        server.executor = executor
        server.start()
    }

    val baseUrl: String
        get() = "http://127.0.0.1:${server.address.port}"

    fun enqueue(path: String, status: Int, body: String, delayMillis: Long = 0) {
        responders.computeIfAbsent(path) { mutableListOf() }.add(Response(status, body, delayMillis))
    }

    fun countOf(path: String): Int = received.count { it.path == path }

    override fun close() {
        server.stop(0)
        executor.shutdownNow()
    }
}
