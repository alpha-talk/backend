package com.alphatalk.worker.price.session

import com.alphatalk.kis.test.FakeKisServer
import com.alphatalk.worker.price.conflation.ConflationBuffer
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.awaitility.Awaitility.await
import java.time.Duration
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

class FixedSubscriptionRunnerTest {
    private val tickFrame =
        "0|H0STCNT0|001|005930^093012^71200^2^700^0.99^71100^70600^71500^70400^71150^71250^15^1234567^87942671300"

    private lateinit var server: FakeKisServer
    private lateinit var buffer: ConflationBuffer
    private lateinit var runner: FixedSubscriptionRunner
    private val mapper = jacksonObjectMapper()

    @BeforeTest
    fun setUp() {
        server = FakeKisServer()
        server.startAndAwait()
        buffer = ConflationBuffer()
        runner = FixedSubscriptionRunner(
            wsUrl = server.url,
            symbols = listOf("005930"),
            approvalKey = { "AK-123" },
            buffer = buffer,
            meters = SimpleMeterRegistry(),
            reconnectDelayMillis = 100,
        )
    }

    @AfterTest
    fun tearDown() {
        runner.stop()
        server.close()
    }

    private fun trTypeOf(message: String): String =
        mapper.readTree(message).path("header").path("tr_type").asText()

    @Test
    fun `기동하면 approval 키로 전 종목을 구독한다`() {
        runner.start()

        server.awaitMessages(1)
        val sent = mapper.readTree(server.receivedMessages[0])
        assertEquals("AK-123", sent.path("header").path("approval_key").asText())
        assertEquals("1", sent.path("header").path("tr_type").asText())
        assertEquals("005930", sent.path("body").path("input").path("tr_key").asText())
    }

    @Test
    fun `수신한 틱은 conflation 버퍼에 쌓인다`() {
        runner.start()
        server.awaitMessages(1)

        server.broadcastText(tickFrame)

        await().atMost(Duration.ofSeconds(5)).until { buffer.drainDirty().isNotEmpty() }
    }

    @Test
    fun `연결이 끊기면 재접속해 다시 구독한다`() {
        runner.start()
        server.awaitMessages(1)

        server.closeAllConnections()

        server.awaitMessages(2, timeoutMillis = 10_000)
        assertEquals("1", trTypeOf(server.receivedMessages[1]))
    }

    @Test
    fun `정지하면 해지 프레임을 보낸다`() {
        runner.start()
        server.awaitMessages(1)

        runner.stop()

        server.awaitMessages(2)
        assertEquals("2", trTypeOf(server.receivedMessages[1]))
    }
}
