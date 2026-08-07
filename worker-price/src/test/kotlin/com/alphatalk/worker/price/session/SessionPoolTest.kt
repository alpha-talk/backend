package com.alphatalk.worker.price.session

import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.test.FakeKisServer
import com.alphatalk.worker.price.conflation.ConflationBuffer
import com.alphatalk.worker.price.market.InMemoryMarketDivStore
import com.alphatalk.worker.price.market.MarketDivStore
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.awaitility.Awaitility.await
import java.time.Duration
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SessionPoolTest {
    private lateinit var server: FakeKisServer
    private val mapper = jacksonObjectMapper()
    private var now = 0L

    @BeforeTest
    fun setUp() {
        server = FakeKisServer()
        server.startAndAwait()
        now = 0L
    }

    @AfterTest
    fun tearDown() {
        server.close()
    }

    private fun pool(
        accounts: Int = 1,
        maxPerSession: Int = 2,
        graceMillis: Long = 1_000,
        ackTimeoutMillis: Long = 5_000,
        trIds: List<String> = listOf("H0STCNT0"),
        marketDivs: MarketDivStore = InMemoryMarketDivStore(),
        silenceMillis: Long = Long.MAX_VALUE,
        meters: SimpleMeterRegistry = SimpleMeterRegistry(),
    ) = SessionPool(
        accounts = (1..accounts).map { KisAccount("key$it", "app$it", "secret$it") },
        wsUrl = server.url,
        approvalKeys = { "AK" },
        buffer = ConflationBuffer(),
        meters = meters,
        tickTrIds = trIds,
        marketDivs = marketDivs,
        silenceMillis = silenceMillis,
        maxRegistrationsPerSession = maxPerSession,
        removalGraceMillis = graceMillis,
        backoff = BackoffPolicy(initialMillis = 50, jitterRatio = 0.0),
        ackTimeoutMillis = ackTimeoutMillis,
        clock = { now },
    )

    private fun ackFrame(code: String, success: Boolean, trId: String = "H0STCNT0"): String {
        val rtCd = if (success) "0" else "1"
        return """{"header":{"tr_id":"$trId","tr_key":"$code","encrypt":"N"},""" +
            """"body":{"rt_cd":"$rtCd","msg_cd":"OPSP0000","msg1":"ack"}}"""
    }

    private fun subscribesOf(messages: List<String>) =
        messages.map { mapper.readTree(it) }.filter { it.path("header").path("tr_type").asText() == "1" }

    private fun unsubscribesOf(messages: List<String>) =
        messages.map { mapper.readTree(it) }.filter { it.path("header").path("tr_type").asText() == "2" }

    private fun awaitConfirmed(meters: SimpleMeterRegistry, count: Int) {
        await().atMost(Duration.ofSeconds(5)).until {
            meters.find("kis.subscribed.symbols").gauge()?.value()?.toInt() == count
        }
    }

    private fun trKeysOf(messages: List<String>, trId: String) =
        subscribesOf(messages)
            .filter { it.path("body").path("input").path("tr_id").asText() == trId }
            .map { it.path("body").path("input").path("tr_key").asText() }

    @Test
    fun `KRX로 확정된 종목은 통합 대신 KRX 체결 TR로 구독한다`() {
        val pool = pool(
            maxPerSession = 4,
            trIds = listOf("H0UNCNT0", "H0STOUP0"),
            marketDivs = InMemoryMarketDivStore(mapOf("047040" to "J")),
        )

        pool.maintain(linkedSetOf("047040", "005930"), subscribeAllowed = true)

        server.awaitMessages(4)
        assertEquals(listOf("005930"), trKeysOf(server.receivedMessages, "H0UNCNT0"))
        assertEquals(listOf("047040"), trKeysOf(server.receivedMessages, "H0STCNT0"))
        assertEquals(setOf("005930", "047040"), trKeysOf(server.receivedMessages, "H0STOUP0").toSet())
    }

    @Test
    fun `통합 등록 뒤 침묵하면 REST 폴링으로 강등한다`() {
        val divs = InMemoryMarketDivStore()
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)

        now += 2_000
        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)

        assertEquals(setOf("047040"), pool.degradedSymbols())
        assertEquals(null, divs.get("047040"))
        assertEquals(listOf("047040"), trKeysOf(server.receivedMessages, "H0UNCNT0"))
    }

    @Test
    fun `분봉이 뒤늦게 KRX로 판정하면 활성 구독도 그 채널로 갈아탄다`() {
        val divs = InMemoryMarketDivStore()
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)
        now += 2_000
        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)
        assertEquals(setOf("047040"), pool.degradedSymbols())

        divs.confirm("047040", "J")
        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)

        server.awaitMessages(3)
        assertEquals(listOf("047040"), trKeysOf(server.receivedMessages, "H0STCNT0"))
        assertTrue(unsubscribesOf(server.receivedMessages).isNotEmpty())
        assertEquals(setOf("047040"), pool.degradedSymbols())

        server.broadcastText(tickFrame("H0STCNT0", "047040"))
        await().atMost(Duration.ofSeconds(5)).until { meters.counter("tick.in").count() > 0 }
        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)

        assertTrue(pool.degradedSymbols().isEmpty())
    }

    @Test
    fun `틱이 흐르는 종목은 구분 기록을 다시 읽지 않는다`() {
        val divs = InMemoryMarketDivStore()
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("005930"), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("005930", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)
        server.broadcastText(tickFrame("H0UNCNT0", "005930"))
        await().atMost(Duration.ofSeconds(5)).until { meters.counter("tick.in").count() > 0 }

        divs.confirm("005930", "J")
        now += 2_000
        pool.maintain(linkedSetOf("005930"), subscribeAllowed = true)

        assertEquals(0.0, meters.counter("tick.div.resubscribed").count())
        assertTrue(trKeysOf(server.receivedMessages, "H0STCNT0").isEmpty())
    }

    @Test
    fun `KRX 틱은 그 종목이 NXT 미상장이라는 증거가 아니라 구분을 기록하지 않는다`() {
        val divs = InMemoryMarketDivStore(mapOf("047040" to "J"))
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)
        divs.confirmed.clear()
        divs.confirm("047040", "J")

        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0STCNT0"))
        awaitConfirmed(meters, 1)

        server.broadcastText(tickFrame("H0STCNT0", "047040"))
        await().atMost(Duration.ofSeconds(5)).until { meters.counter("tick.in").count() > 0 }

        assertTrue(pool.degradedSymbols().isEmpty())
        assertEquals(0.0, meters.counter("tick.market.div", "div", "J").count())
    }

    @Test
    fun `통합 틱이 오면 그 종목을 통합으로 확정 기록한다`() {
        val divs = InMemoryMarketDivStore()
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("005930"), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("005930", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)

        server.broadcastText(tickFrame("H0UNCNT0", "005930"))

        await().atMost(Duration.ofSeconds(5)).until { divs.get("005930") == "UN" }
    }

    @Test
    fun `전환 전 채널의 잔여 틱은 침묵 상태를 되돌리지 않는다`() {
        val divs = InMemoryMarketDivStore(mapOf("047040" to "J"))
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0STCNT0"))
        awaitConfirmed(meters, 1)
        now += 2_000
        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)
        assertEquals(setOf("047040"), pool.degradedSymbols())

        server.broadcastText(tickFrame("H0UNCNT0", "047040"))
        await().atMost(Duration.ofSeconds(5)).until { meters.counter("tick.in").count() > 0 }

        now += 2_000
        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)

        assertEquals(setOf("047040"), pool.degradedSymbols())
        assertEquals("J", divs.get("047040"))
    }


    private fun tickFrame(trId: String, code: String) =
        "0|$trId|001|$code^134058^16110^2^10^0.06^16000^16200^16000^0^0^0^0^4355991"

    @Test
    fun `시간외 틱은 통합 체결 증거가 아니라 구분을 확정하지 않는다`() {
        val divs = InMemoryMarketDivStore()
        val meters = SimpleMeterRegistry()
        val pool = pool(
            maxPerSession = 4,
            trIds = listOf("H0UNCNT0", "H0STOUP0"),
            marketDivs = divs,
            silenceMillis = 1_000,
            meters = meters,
        )

        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)
        server.awaitMessages(2)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)

        server.broadcastText(tickFrame("H0STOUP0", "047040"))
        await().atMost(Duration.ofSeconds(5)).until { meters.counter("tick.in").count() > 0 }

        assertEquals(null, divs.get("047040"))
        assertEquals(0.0, meters.counter("tick.market.div", "div", "UN").count())
    }

    @Test
    fun `시간외 틱은 침묵 감지를 막지 못한다`() {
        val divs = InMemoryMarketDivStore()
        val meters = SimpleMeterRegistry()
        val pool = pool(
            maxPerSession = 4,
            trIds = listOf("H0UNCNT0", "H0STOUP0"),
            marketDivs = divs,
            silenceMillis = 1_000,
            meters = meters,
        )

        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)
        server.awaitMessages(2)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)
        server.broadcastText(tickFrame("H0STOUP0", "047040"))
        await().atMost(Duration.ofSeconds(5)).until { meters.counter("tick.in").count() > 0 }

        now += 2_000
        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)

        assertEquals(1.0, meters.counter("tick.silence.degraded").count())
    }

    @Test
    fun `이미 KRX로 확정된 종목이 침묵하면 전환 단계 없이 바로 강등한다`() {
        val divs = InMemoryMarketDivStore(mapOf("047040" to "J"))
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0STCNT0"))
        awaitConfirmed(meters, 1)

        now += 2_000
        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)

        assertEquals(setOf("047040"), pool.degradedSymbols())
    }

    @Test
    fun `재접속하면 침묵 감시를 다시 무장하되 학습한 구분은 유지한다`() {
        val divs = InMemoryMarketDivStore()
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)
        now += 2_000
        pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)
        assertEquals(setOf("047040"), pool.degradedSymbols())

        server.closeAllConnections()

        await().atMost(Duration.ofSeconds(10)).until {
            now += 200
            pool.maintain(linkedSetOf("047040"), subscribeAllowed = true)
            trKeysOf(server.receivedMessages, "H0UNCNT0").size >= 2
        }

        assertTrue(pool.degradedSymbols().isEmpty())
    }

    @Test
    fun `용량을 넘는 종목은 강등 목록에 남는다`() {
        val pool = pool(accounts = 1, maxPerSession = 2)

        pool.maintain(linkedSetOf("000001", "000002", "000003"), subscribeAllowed = true)

        server.awaitMessages(2)
        assertEquals(2, subscribesOf(server.receivedMessages).size)
        assertEquals(setOf("000003"), pool.degradedSymbols())
    }

    @Test
    fun `종목은 빈 슬롯이 많은 세션부터 배정된다`() {
        val pool = pool(accounts = 2, maxPerSession = 2)

        pool.maintain(linkedSetOf("000001", "000002", "000003"), subscribeAllowed = true)

        server.awaitConnections(2)
        server.awaitMessages(3)
        assertEquals(3, subscribesOf(server.receivedMessages).size)
        assertTrue(pool.degradedSymbols().isEmpty())
    }

    @Test
    fun `구독 허용 전에는 연결만 하고 구독하지 않는다`() {
        val pool = pool()

        pool.maintain(setOf("005930"), subscribeAllowed = false)

        server.awaitConnections(1)
        Thread.sleep(200)
        assertTrue(server.receivedMessages.isEmpty())
    }

    @Test
    fun `절단되면 백오프 후 재접속해 배정분을 재구독한다`() {
        val pool = pool()
        pool.maintain(setOf("005930"), subscribeAllowed = true)
        server.awaitMessages(1)

        server.closeAllConnections()

        await().atMost(Duration.ofSeconds(10)).until {
            now += 200
            pool.maintain(setOf("005930"), subscribeAllowed = true)
            subscribesOf(server.receivedMessages).size >= 2
        }
    }

    @Test
    fun `전송 계층 오류로만 끊겨도 재접속해 재구독한다`() {
        val pool = pool()
        pool.maintain(setOf("005930"), subscribeAllowed = true)
        server.awaitMessages(1)

        server.abortAllConnections()

        await().atMost(Duration.ofSeconds(10)).until {
            now += 200
            pool.maintain(setOf("005930"), subscribeAllowed = true)
            subscribesOf(server.receivedMessages).size >= 2
        }
    }

    @Test
    fun `구독이 거절되면 다음 리컨실에서 재등록한다`() {
        val pool = pool()
        pool.maintain(setOf("000001"), subscribeAllowed = true)
        server.awaitMessages(1)
        assertEquals(1, subscribesOf(server.receivedMessages).size)

        server.broadcastText(ackFrame("000001", success = false))

        await().atMost(Duration.ofSeconds(10)).until {
            pool.maintain(setOf("000001"), subscribeAllowed = true)
            subscribesOf(server.receivedMessages).size >= 2
        }
    }

    @Test
    fun `구독이 확정되면 ACK 유효기간이 지나도 재등록하지 않는다`() {
        val pool = pool(ackTimeoutMillis = 100)
        pool.maintain(setOf("000001"), subscribeAllowed = true)
        server.awaitMessages(1)

        server.broadcastText(ackFrame("000001", success = true))
        Thread.sleep(300)
        now += 500
        repeat(3) { pool.maintain(setOf("000001"), subscribeAllowed = true) }

        Thread.sleep(200)
        assertEquals(1, subscribesOf(server.receivedMessages).size)
    }

    @Test
    fun `응답이 없으면 ACK 유효기간 뒤에 재등록한다`() {
        val pool = pool(ackTimeoutMillis = 100)
        pool.maintain(setOf("000001"), subscribeAllowed = true)
        server.awaitMessages(1)

        now += 500
        pool.maintain(setOf("000001"), subscribeAllowed = true)

        server.awaitMessages(2)
        assertEquals(2, subscribesOf(server.receivedMessages).size)
    }

    @Test
    fun `해지는 유예가 지난 뒤에만 전송된다`() {
        val pool = pool(graceMillis = 1_000)
        pool.maintain(linkedSetOf("000001", "000002"), subscribeAllowed = true)
        server.awaitMessages(2)

        pool.maintain(setOf("000001"), subscribeAllowed = true)
        Thread.sleep(200)
        assertTrue(unsubscribesOf(server.receivedMessages).isEmpty())

        now += 1_500
        pool.maintain(setOf("000001"), subscribeAllowed = true)

        server.awaitMessages(3)
        val unsubscribed = unsubscribesOf(server.receivedMessages)
        assertEquals(1, unsubscribed.size)
        assertEquals("000002", unsubscribed[0].path("body").path("input").path("tr_key").asText())
    }

    @Test
    fun `유예 중 재수요가 오면 해지가 취소된다`() {
        val pool = pool(graceMillis = 1_000)
        pool.maintain(linkedSetOf("000001", "000002"), subscribeAllowed = true)
        server.awaitMessages(2)

        pool.maintain(setOf("000001"), subscribeAllowed = true)
        pool.maintain(linkedSetOf("000001", "000002"), subscribeAllowed = true)
        now += 2_000
        pool.maintain(linkedSetOf("000001", "000002"), subscribeAllowed = true)

        Thread.sleep(200)
        assertTrue(unsubscribesOf(server.receivedMessages).isEmpty())
    }

    @Test
    fun `disconnectAll은 해지 후 연결을 닫는다`() {
        val pool = pool()
        pool.maintain(setOf("005930"), subscribeAllowed = true)
        server.awaitMessages(1)

        pool.disconnectAll()

        server.awaitMessages(2)
        assertEquals(1, unsubscribesOf(server.receivedMessages).size)
        await().atMost(Duration.ofSeconds(5)).until { server.connectionCount == 0 }
    }

    @Test
    fun `TR이 여러 개면 심볼당 TR별로 모두 등록한다`() {
        val pool = pool(maxPerSession = 4, trIds = listOf("H0UNCNT0", "H0STOUP0"))

        pool.maintain(setOf("005930"), subscribeAllowed = true)

        server.awaitMessages(2)
        val subscribes = subscribesOf(server.receivedMessages)
        assertEquals(
            setOf("H0UNCNT0" to "005930", "H0STOUP0" to "005930"),
            subscribes.map {
                it.path("body").path("input").path("tr_id").asText() to
                    it.path("body").path("input").path("tr_key").asText()
            }.toSet(),
        )
    }

    @Test
    fun `심볼 용량은 등록 한도를 TR 수로 나눠 계산한다`() {
        val pool = pool(maxPerSession = 4, trIds = listOf("H0UNCNT0", "H0STOUP0"))

        pool.maintain(linkedSetOf("000001", "000002", "000003"), subscribeAllowed = true)

        server.awaitMessages(4)
        assertEquals(4, subscribesOf(server.receivedMessages).size)
        assertEquals(setOf("000003"), pool.degradedSymbols())
    }

    @Test
    fun `해지 시 심볼의 모든 TR을 해제한다`() {
        val pool = pool(maxPerSession = 4, graceMillis = 1_000, trIds = listOf("H0UNCNT0", "H0STOUP0"))
        pool.maintain(linkedSetOf("000001", "000002"), subscribeAllowed = true)
        server.awaitMessages(4)

        pool.maintain(setOf("000001"), subscribeAllowed = true)
        now += 1_500
        pool.maintain(setOf("000001"), subscribeAllowed = true)

        server.awaitMessages(6)
        val unsubscribed = unsubscribesOf(server.receivedMessages)
        assertEquals(
            setOf("H0UNCNT0" to "000002", "H0STOUP0" to "000002"),
            unsubscribed.map {
                it.path("body").path("input").path("tr_id").asText() to
                    it.path("body").path("input").path("tr_key").asText()
            }.toSet(),
        )
    }

    @Test
    fun `ACK는 TR 단위로 확정되고 응답 없는 TR만 재등록한다`() {
        val pool = pool(maxPerSession = 4, ackTimeoutMillis = 100, trIds = listOf("H0UNCNT0", "H0STOUP0"))
        pool.maintain(setOf("000001"), subscribeAllowed = true)
        server.awaitMessages(2)

        server.broadcastText(ackFrame("000001", success = true, trId = "H0UNCNT0"))
        Thread.sleep(300)
        now += 500
        pool.maintain(setOf("000001"), subscribeAllowed = true)

        server.awaitMessages(3)
        Thread.sleep(200)
        val resubscribed = subscribesOf(server.receivedMessages).drop(2)
        assertEquals(1, resubscribed.size)
        assertEquals("H0STOUP0", resubscribed[0].path("body").path("input").path("tr_id").asText())
    }
}
