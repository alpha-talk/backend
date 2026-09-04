package com.alphatalk.kis.ws

import com.alphatalk.kis.test.FakeKisServer
import com.alphatalk.kis.test.StallingHandshakeServer
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.concurrent.CompletionException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KisWebSocketSessionTest {
    private lateinit var server: FakeKisServer
    private lateinit var listener: RecordingListener
    private lateinit var session: KisWebSocketSession
    private val mapper = jacksonObjectMapper()

    @BeforeTest
    fun setUp() {
        server = FakeKisServer()
        server.startAndAwait()
        listener = RecordingListener()
        session = KisWebSocketSession(server.url, "AK-123", listener)
        session.connect().get(5, TimeUnit.SECONDS)
        server.awaitConnections(1)
    }

    @AfterTest
    fun tearDown() {
        session.close()
        server.close()
    }

    private fun fixture(name: String): String =
        javaClass.getResource("/fixtures/$name")!!.readText()

    @Test
    fun `구독 요청은 approval_key와 tr_type 1을 담은 JSON 프레임이다`() {
        session.subscribe("005930")

        server.awaitMessages(1)
        val sent = mapper.readTree(server.receivedMessages[0])
        assertEquals("AK-123", sent.path("header").path("approval_key").asText())
        assertEquals("P", sent.path("header").path("custtype").asText())
        assertEquals("1", sent.path("header").path("tr_type").asText())
        assertEquals("H0STCNT0", sent.path("body").path("input").path("tr_id").asText())
        assertEquals("005930", sent.path("body").path("input").path("tr_key").asText())
    }

    @Test
    fun `해지 요청은 tr_type 2다`() {
        session.unsubscribe("005930")

        server.awaitMessages(1)
        val sent = mapper.readTree(server.receivedMessages[0])
        assertEquals("2", sent.path("header").path("tr_type").asText())
    }

    @Test
    fun `틱 프레임이 리스너로 전달된다`() {
        server.broadcastText(fixture("h0stcnt0-double.txt"))

        awaitTrue { listener.ticks.size == 2 }
        assertEquals("005930", listener.ticks[0].code)
        assertEquals("000660", listener.ticks[1].code)
    }

    @Test
    fun `PINGPONG은 동일 프레임으로 에코된다`() {
        val pingpong = fixture("pingpong.txt")

        server.broadcastText(pingpong)

        server.awaitMessages(1)
        assertEquals(pingpong, server.receivedMessages[0])
        awaitTrue { listener.pingPongCount.get() == 1 }
    }

    @Test
    fun `구독 응답이 리스너로 전달된다`() {
        server.broadcastText(fixture("subscribe-success.txt"))

        awaitTrue { listener.acks.size == 1 }
        assertEquals(Triple("H0STCNT0", "005930", true), listener.acks[0])
    }

    @Test
    fun `서버가 연결을 닫으면 onClosed가 호출된다`() {
        server.closeAllConnections()

        awaitTrue { listener.closedReasons.isNotEmpty() }
    }

    @Test
    fun `비정상 절단도 종료나 전송 오류 신호로 전달된다`() {
        server.abortAllConnections()

        awaitTrue { listener.closedReasons.isNotEmpty() || listener.transportErrors.isNotEmpty() }
    }

    @Test
    fun `abort는 close 핸드셰이크 없이 연결을 끊는다`() {
        session.abort()

        awaitTrue { server.connectionCount == 0 }
        assertFalse(session.isOpen)
    }

    @Test
    fun `핸드셰이크 전에 abort된 세션은 늦게 열린 소켓을 즉시 끊는다`() {
        StallingHandshakeServer().use { stalling ->
            val orphan = KisWebSocketSession(stalling.url, "AK-123", RecordingListener())
            val handshake = orphan.connect()
            stalling.awaitConnections(1)

            orphan.abort()
            stalling.completeHandshakes()

            stalling.awaitPeersClosed()
            assertTrue(runCatching { handshake.get(5, TimeUnit.SECONDS) }.isFailure)
            assertFalse(orphan.isOpen)
        }
    }

    @Test
    fun `핸드셰이크가 끝나지 않으면 제한 시간 뒤 실패하고 연결을 닫는다`() {
        StallingHandshakeServer().use { stalling ->
            val stalled = KisWebSocketSession(stalling.url, "AK-123", RecordingListener())

            val handshake = stalled.connect(Duration.ofSeconds(1))

            val failure = runCatching { handshake.get(5, TimeUnit.SECONDS) }.exceptionOrNull()
            assertTrue(rootCause(failure) is HttpTimeoutException, "unexpected failure: $failure")
            stalling.awaitPeersClosed()
            assertFalse(stalled.isOpen)
        }
    }

    private fun rootCause(t: Throwable?): Throwable? =
        generateSequence(t) { it.cause?.takeIf { cause -> cause !== it } }
            .lastOrNull { it !is java.util.concurrent.ExecutionException && it !is CompletionException }

    private fun awaitTrue(timeoutMillis: Long = 5000, condition: () -> Boolean) {
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (System.currentTimeMillis() < deadline) {
            if (condition()) return
            Thread.sleep(20)
        }
        assertTrue(condition())
    }

    private class RecordingListener : KisSessionListener {
        val ticks = CopyOnWriteArrayList<KisTick>()
        val acks = CopyOnWriteArrayList<Triple<String?, String?, Boolean>>()
        val closedReasons = CopyOnWriteArrayList<String?>()
        val transportErrors = CopyOnWriteArrayList<Throwable>()
        val pingPongCount = AtomicInteger()

        override fun onTransportError(t: Throwable) {
            transportErrors += t
        }

        override fun onTicks(trId: String, ticks: List<KisTick>) {
            this.ticks += ticks
        }

        override fun onSubscribeAck(trId: String?, trKey: String?, success: Boolean) {
            acks += Triple(trId, trKey, success)
        }

        override fun onPingPong() {
            pingPongCount.incrementAndGet()
        }

        override fun onClosed(reason: String?) {
            closedReasons += reason
        }
    }
}
