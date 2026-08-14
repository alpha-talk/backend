package com.alphatalk.worker.price.session

import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.test.FakeKisServer
import com.alphatalk.worker.price.conflation.ConflationBuffer
import com.alphatalk.worker.price.conflation.DepthConflationBuffer
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
        buffer: ConflationBuffer = ConflationBuffer(),
        depthBuffer: DepthConflationBuffer = DepthConflationBuffer(),
        depthEnabled: Boolean = true,
    ) = SessionPool(
        accounts = (1..accounts).map { KisAccount("key$it", "app$it", "secret$it") },
        wsUrl = server.url,
        approvalKeys = { "AK" },
        buffer = buffer,
        meters = meters,
        tickTrIds = trIds,
        marketDivs = marketDivs,
        depthBuffer = depthBuffer,
        depthEnabled = depthEnabled,
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

    private fun awaitTicksReceived(meters: SimpleMeterRegistry, count: Int) {
        await().atMost(Duration.ofSeconds(5)).until { meters.counter("tick.in").count().toInt() >= count }
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

        pool.maintain(linkedSetOf("047040", "005930"), emptyList(), subscribeAllowed = true)

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

        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)

        now += 2_000
        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)

        assertEquals(setOf("047040"), pool.degradedSymbols())
        assertEquals(null, divs.get("047040"))
        assertEquals(listOf("047040"), trKeysOf(server.receivedMessages, "H0UNCNT0"))
    }

    @Test
    fun `분봉이 뒤늦게 KRX로 판정하면 활성 구독도 그 채널로 갈아탄다`() {
        val divs = InMemoryMarketDivStore()
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)
        now += 2_000
        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
        assertEquals(setOf("047040"), pool.degradedSymbols())

        divs.confirm("047040", "J")
        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)

        server.awaitMessages(3)
        assertEquals(listOf("047040"), trKeysOf(server.receivedMessages, "H0STCNT0"))
        assertTrue(unsubscribesOf(server.receivedMessages).isNotEmpty())
        assertEquals(setOf("047040"), pool.degradedSymbols())

        server.broadcastText(tickFrame("H0STCNT0", "047040"))
        await().atMost(Duration.ofSeconds(5)).until { meters.counter("tick.in").count() > 0 }
        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)

        assertTrue(pool.degradedSymbols().isEmpty())
    }

    @Test
    fun `전환 직전에 도착한 이전 채널 틱은 전환 뒤 폐기된다`() {
        val divs = InMemoryMarketDivStore()
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)
        now += 2_000
        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
        assertEquals(setOf("047040"), pool.degradedSymbols())

        server.broadcastText(tickFrame("H0UNCNT0", "047040"))
        awaitTicksReceived(meters, 1)
        divs.confirm("047040", "J")

        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)

        assertEquals(setOf("047040"), pool.degradedSymbols())
        assertEquals("J", divs.get("047040"))

        now += 2_000
        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
        assertEquals(setOf("047040"), pool.degradedSymbols())
    }

    @Test
    fun `현재 채널 틱은 다음 정비 주기에 흡수되어 강등을 푼다`() {
        val divs = InMemoryMarketDivStore()
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("005930"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("005930", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)
        now += 2_000
        pool.maintain(linkedSetOf("005930"), emptyList(), subscribeAllowed = true)
        assertEquals(setOf("005930"), pool.degradedSymbols())

        server.broadcastText(tickFrame("H0UNCNT0", "005930"))
        awaitTicksReceived(meters, 1)
        pool.maintain(linkedSetOf("005930"), emptyList(), subscribeAllowed = true)

        assertTrue(pool.degradedSymbols().isEmpty())
        assertEquals("UN", divs.get("005930"))
    }

    @Test
    fun `틱이 흐르는 종목은 구분 기록을 다시 읽지 않는다`() {
        val divs = InMemoryMarketDivStore()
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("005930"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("005930", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)
        server.broadcastText(tickFrame("H0UNCNT0", "005930"))
        awaitTicksReceived(meters, 1)
        pool.maintain(linkedSetOf("005930"), emptyList(), subscribeAllowed = true)

        divs.confirm("005930", "J")
        now += 2_000
        pool.maintain(linkedSetOf("005930"), emptyList(), subscribeAllowed = true)

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

        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
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

        pool.maintain(linkedSetOf("005930"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("005930", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)

        server.broadcastText(tickFrame("H0UNCNT0", "005930"))
        awaitTicksReceived(meters, 1)
        pool.maintain(linkedSetOf("005930"), emptyList(), subscribeAllowed = true)

        assertEquals("UN", divs.get("005930"))
    }

    @Test
    fun `전환 전 채널의 잔여 틱은 침묵 상태를 되돌리지 않는다`() {
        val divs = InMemoryMarketDivStore(mapOf("047040" to "J"))
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0STCNT0"))
        awaitConfirmed(meters, 1)
        now += 2_000
        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
        assertEquals(setOf("047040"), pool.degradedSymbols())

        server.broadcastText(tickFrame("H0UNCNT0", "047040"))
        await().atMost(Duration.ofSeconds(5)).until { meters.counter("tick.in").count() > 0 }

        now += 2_000
        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)

        assertEquals(setOf("047040"), pool.degradedSymbols())
        assertEquals("J", divs.get("047040"))
    }


    private fun tickFrame(trId: String, code: String) =
        "0|$trId|001|$code^134058^16110^2^10^0.06^16000^16200^16000^0^0^0^0^4355991"

    private fun depthFrame(trId: String, code: String): String {
        val fields = mutableListOf(code, "093012", "0")
        fields += (0 until 10).map { (71300 + it * 100).toString() }
        fields += (0 until 10).map { (71200 - it * 100).toString() }
        fields += (0 until 10).map { (100 + it).toString() }
        fields += (0 until 10).map { (200 + it).toString() }
        fields += listOf("1810", "2400", "0", "0", "71250", "1200", "34567", "750", "2", "1.06", "1234567", "15", "-20", "0", "0", "00")
        return "0|$trId|001|" + fields.joinToString("^")
    }

    @Test
    fun `방 수요 종목은 잔여 슬롯 안에서 호가 TR을 등록한다`() {
        val pool = pool(maxPerSession = 3, trIds = listOf("H0UNCNT0"))

        pool.maintain(linkedSetOf("005930", "000660"), listOf("005930"), subscribeAllowed = true)

        server.awaitMessages(3)
        assertEquals(listOf("005930"), trKeysOf(server.receivedMessages, "H0UNASP0"))
    }

    @Test
    fun `잔여 슬롯을 넘는 방 수요는 우선순위 순으로 등록하고 초과분은 드랍 게이지에 남긴다`() {
        val meters = SimpleMeterRegistry()
        val pool = pool(maxPerSession = 3, trIds = listOf("H0UNCNT0"), meters = meters)

        pool.maintain(linkedSetOf("005930", "000660"), listOf("000660", "005930"), subscribeAllowed = true)

        server.awaitMessages(3)
        assertEquals(listOf("000660"), trKeysOf(server.receivedMessages, "H0UNASP0"))
        assertEquals(1.0, meters.find("depth.symbols").gauge()?.value())
        assertEquals(1.0, meters.find("depth.symbols.dropped").gauge()?.value())
    }

    @Test
    fun `KRX로 확정된 종목의 호가는 KRX 호가 TR로 등록한다`() {
        val pool = pool(
            maxPerSession = 4,
            trIds = listOf("H0UNCNT0"),
            marketDivs = InMemoryMarketDivStore(mapOf("047040" to "J")),
        )

        pool.maintain(linkedSetOf("047040"), listOf("047040"), subscribeAllowed = true)

        server.awaitMessages(2)
        assertEquals(listOf("047040"), trKeysOf(server.receivedMessages, "H0STASP0"))
        assertTrue(trKeysOf(server.receivedMessages, "H0UNASP0").isEmpty())
    }

    @Test
    fun `방 수요가 사라지면 유예 뒤 호가만 해지한다`() {
        val pool = pool(maxPerSession = 4, trIds = listOf("H0UNCNT0"), graceMillis = 1_000)
        pool.maintain(setOf("005930"), listOf("005930"), subscribeAllowed = true)
        server.awaitMessages(2)

        pool.maintain(setOf("005930"), emptyList(), subscribeAllowed = true)
        Thread.sleep(200)
        assertTrue(unsubscribesOf(server.receivedMessages).isEmpty())

        now += 1_500
        pool.maintain(setOf("005930"), emptyList(), subscribeAllowed = true)

        server.awaitMessages(3)
        val unsubscribed = unsubscribesOf(server.receivedMessages)
        assertEquals(1, unsubscribed.size)
        assertEquals("H0UNASP0", unsubscribed[0].path("body").path("input").path("tr_id").asText())
        assertEquals("005930", unsubscribed[0].path("body").path("input").path("tr_key").asText())
    }

    @Test
    fun `유예 중인 호가 등록은 신규 방 수요보다 슬롯을 먼저 지킨다`() {
        val meters = SimpleMeterRegistry()
        val pool = pool(maxPerSession = 3, trIds = listOf("H0UNCNT0"), graceMillis = 1_000, meters = meters)
        pool.maintain(linkedSetOf("005930", "000660"), listOf("005930"), subscribeAllowed = true)
        server.awaitMessages(3)

        pool.maintain(linkedSetOf("005930", "000660"), listOf("000660"), subscribeAllowed = true)

        Thread.sleep(200)
        assertTrue(unsubscribesOf(server.receivedMessages).isEmpty())
        assertEquals(listOf("005930"), trKeysOf(server.receivedMessages, "H0UNASP0"))
        assertEquals(1.0, meters.find("depth.symbols.dropped").gauge()?.value())

        now += 2_000
        pool.maintain(linkedSetOf("005930", "000660"), listOf("000660"), subscribeAllowed = true)

        server.awaitMessages(5)
        assertEquals(listOf("005930", "000660"), trKeysOf(server.receivedMessages, "H0UNASP0"))
        val unsubscribed = unsubscribesOf(server.receivedMessages)
        assertEquals(1, unsubscribed.size)
        assertEquals("H0UNASP0", unsubscribed[0].path("body").path("input").path("tr_id").asText())
        assertEquals("005930", unsubscribed[0].path("body").path("input").path("tr_key").asText())
    }

    @Test
    fun `가득 찬 세션에서 방 수요는 quote 전용 등록을 선점하고 밀린 종목은 강등된다`() {
        val meters = SimpleMeterRegistry()
        val pool = pool(maxPerSession = 2, trIds = listOf("H0UNCNT0"), meters = meters)
        pool.maintain(linkedSetOf("005930", "000660"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(2)

        pool.maintain(linkedSetOf("005930", "000660", "035420"), listOf("035420"), subscribeAllowed = true)

        server.awaitMessages(4)
        assertEquals(listOf("005930", "000660", "035420"), trKeysOf(server.receivedMessages, "H0UNCNT0"))
        val unsubscribed = unsubscribesOf(server.receivedMessages)
        assertEquals(1, unsubscribed.size)
        assertEquals("005930", unsubscribed[0].path("body").path("input").path("tr_key").asText())
        assertEquals(setOf("005930"), pool.degradedSymbols())
        assertEquals(1.0, meters.counter("tick.room.preempted").count())
    }

    @Test
    fun `용량이 줄면 유예 홀드오버가 아니라 유지 중인 방 수요가 슬롯을 지킨다`() {
        val pool = pool(maxPerSession = 4, trIds = listOf("H0UNCNT0"), graceMillis = 1_000)
        pool.maintain(linkedSetOf("005930", "000660"), listOf("005930", "000660"), subscribeAllowed = true)
        server.awaitMessages(4)

        pool.maintain(linkedSetOf("005930", "000660", "035420"), listOf("005930"), subscribeAllowed = true)

        server.awaitMessages(6)
        val unsubscribed = unsubscribesOf(server.receivedMessages)
        assertEquals(1, unsubscribed.size)
        assertEquals("H0UNASP0", unsubscribed[0].path("body").path("input").path("tr_id").asText())
        assertEquals("000660", unsubscribed[0].path("body").path("input").path("tr_key").asText())
        assertEquals(listOf("005930", "000660"), trKeysOf(server.receivedMessages, "H0UNASP0"))
    }

    @Test
    fun `틱 수요가 슬롯을 되찾으면 같은 정비에서 호가 해지가 틱 등록보다 먼저 나간다`() {
        val pool = pool(maxPerSession = 2, trIds = listOf("H0UNCNT0"), graceMillis = 1_000)
        pool.maintain(linkedSetOf("005930"), listOf("005930"), subscribeAllowed = true)
        server.awaitMessages(2)

        pool.maintain(linkedSetOf("005930", "000660"), emptyList(), subscribeAllowed = true)

        server.awaitMessages(4)
        val messages = server.receivedMessages.map { mapper.readTree(it) }
        val depthUnsubIndex = messages.indexOfFirst {
            it.path("header").path("tr_type").asText() == "2" &&
                it.path("body").path("input").path("tr_id").asText() == "H0UNASP0"
        }
        val newTickSubIndex = messages.indexOfFirst {
            it.path("header").path("tr_type").asText() == "1" &&
                it.path("body").path("input").path("tr_key").asText() == "000660"
        }
        assertTrue(depthUnsubIndex >= 0)
        assertTrue(newTickSubIndex > depthUnsubIndex)
    }

    @Test
    fun `유예 중 방 수요가 돌아오면 호가 해지가 취소된다`() {
        val pool = pool(maxPerSession = 4, trIds = listOf("H0UNCNT0"), graceMillis = 1_000)
        pool.maintain(setOf("005930"), listOf("005930"), subscribeAllowed = true)
        server.awaitMessages(2)

        pool.maintain(setOf("005930"), emptyList(), subscribeAllowed = true)
        pool.maintain(setOf("005930"), listOf("005930"), subscribeAllowed = true)
        now += 2_000
        pool.maintain(setOf("005930"), listOf("005930"), subscribeAllowed = true)

        Thread.sleep(200)
        assertTrue(unsubscribesOf(server.receivedMessages).isEmpty())
    }

    @Test
    fun `분봉이 KRX로 판정하면 호가 등록도 함께 갈아탄다`() {
        val divs = InMemoryMarketDivStore()
        val meters = SimpleMeterRegistry()
        val pool = pool(
            maxPerSession = 4,
            trIds = listOf("H0UNCNT0"),
            marketDivs = divs,
            silenceMillis = 1_000,
            meters = meters,
        )
        pool.maintain(linkedSetOf("047040"), listOf("047040"), subscribeAllowed = true)
        server.awaitMessages(2)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)
        now += 2_000
        pool.maintain(linkedSetOf("047040"), listOf("047040"), subscribeAllowed = true)

        divs.confirm("047040", "J")
        pool.maintain(linkedSetOf("047040"), listOf("047040"), subscribeAllowed = true)

        await().atMost(Duration.ofSeconds(5)).until {
            trKeysOf(server.receivedMessages, "H0STASP0") == listOf("047040")
        }
        assertEquals(listOf("047040"), trKeysOf(server.receivedMessages, "H0STCNT0"))
    }

    @Test
    fun `호가 프레임은 호가 버퍼로 들어가고 침묵 판정을 되돌리지 않는다`() {
        val meters = SimpleMeterRegistry()
        val depthBuffer = DepthConflationBuffer()
        val pool = pool(
            maxPerSession = 4,
            trIds = listOf("H0UNCNT0"),
            silenceMillis = 1_000,
            meters = meters,
            depthBuffer = depthBuffer,
        )
        pool.maintain(linkedSetOf("005930"), listOf("005930"), subscribeAllowed = true)
        server.awaitMessages(2)
        server.broadcastText(ackFrame("005930", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)

        server.broadcastText(depthFrame("H0UNASP0", "005930"))
        await().atMost(Duration.ofSeconds(5)).until { meters.counter("depth.in").count() > 0 }

        val drained = depthBuffer.drainDirty()
        assertEquals(setOf("005930"), drained.keys)
        assertEquals(listOf(71300L, 100L), drained.getValue("005930").asks[0])

        now += 2_000
        pool.maintain(linkedSetOf("005930"), listOf("005930"), subscribeAllowed = true)

        assertEquals(setOf("005930"), pool.degradedSymbols())
        assertEquals(0.0, meters.counter("tick.in").count())
    }

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

        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
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

        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(2)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)
        server.broadcastText(tickFrame("H0STOUP0", "047040"))
        await().atMost(Duration.ofSeconds(5)).until { meters.counter("tick.in").count() > 0 }

        now += 2_000
        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)

        assertEquals(1.0, meters.counter("tick.silence.degraded").count())
    }

    @Test
    fun `이미 KRX로 확정된 종목이 침묵하면 전환 단계 없이 바로 강등한다`() {
        val divs = InMemoryMarketDivStore(mapOf("047040" to "J"))
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0STCNT0"))
        awaitConfirmed(meters, 1)

        now += 2_000
        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)

        assertEquals(setOf("047040"), pool.degradedSymbols())
    }


    @Test
    fun `강등된 종목은 재접속을 거쳐도 틱이 다시 흐를 때까지 REST 폴백을 유지한다`() {
        val divs = InMemoryMarketDivStore()
        val meters = SimpleMeterRegistry()
        val pool = pool(trIds = listOf("H0UNCNT0"), marketDivs = divs, silenceMillis = 1_000, meters = meters)

        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)
        server.broadcastText(ackFrame("047040", success = true, trId = "H0UNCNT0"))
        awaitConfirmed(meters, 1)
        now += 2_000
        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
        assertEquals(setOf("047040"), pool.degradedSymbols())

        server.closeAllConnections()
        await().atMost(Duration.ofSeconds(10)).until {
            now += 200
            pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)
            trKeysOf(server.receivedMessages, "H0UNCNT0").size >= 2
        }

        assertEquals(setOf("047040"), pool.degradedSymbols())

        server.broadcastText(tickFrame("H0UNCNT0", "047040"))
        awaitTicksReceived(meters, 1)
        pool.maintain(linkedSetOf("047040"), emptyList(), subscribeAllowed = true)

        assertTrue(pool.degradedSymbols().isEmpty())
    }

    @Test
    fun `용량을 넘는 종목은 강등 목록에 남는다`() {
        val pool = pool(accounts = 1, maxPerSession = 2)

        pool.maintain(linkedSetOf("000001", "000002", "000003"), emptyList(), subscribeAllowed = true)

        server.awaitMessages(2)
        assertEquals(2, subscribesOf(server.receivedMessages).size)
        assertEquals(setOf("000003"), pool.degradedSymbols())
    }

    @Test
    fun `종목은 빈 슬롯이 많은 세션부터 배정된다`() {
        val pool = pool(accounts = 2, maxPerSession = 2)

        pool.maintain(linkedSetOf("000001", "000002", "000003"), emptyList(), subscribeAllowed = true)

        server.awaitConnections(2)
        server.awaitMessages(3)
        assertEquals(3, subscribesOf(server.receivedMessages).size)
        assertTrue(pool.degradedSymbols().isEmpty())
    }

    @Test
    fun `구독 허용 전에는 연결만 하고 구독하지 않는다`() {
        val pool = pool()

        pool.maintain(setOf("005930"), emptyList(), subscribeAllowed = false)

        server.awaitConnections(1)
        Thread.sleep(200)
        assertTrue(server.receivedMessages.isEmpty())
    }

    @Test
    fun `절단되면 백오프 후 재접속해 배정분을 재구독한다`() {
        val pool = pool()
        pool.maintain(setOf("005930"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)

        server.closeAllConnections()

        await().atMost(Duration.ofSeconds(10)).until {
            now += 200
            pool.maintain(setOf("005930"), emptyList(), subscribeAllowed = true)
            subscribesOf(server.receivedMessages).size >= 2
        }
    }

    @Test
    fun `전송 계층 오류로만 끊겨도 재접속해 재구독한다`() {
        val pool = pool()
        pool.maintain(setOf("005930"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)

        server.abortAllConnections()

        await().atMost(Duration.ofSeconds(10)).until {
            now += 200
            pool.maintain(setOf("005930"), emptyList(), subscribeAllowed = true)
            subscribesOf(server.receivedMessages).size >= 2
        }
    }

    @Test
    fun `구독이 거절되면 다음 리컨실에서 재등록한다`() {
        val pool = pool()
        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)
        assertEquals(1, subscribesOf(server.receivedMessages).size)

        server.broadcastText(ackFrame("000001", success = false))

        await().atMost(Duration.ofSeconds(10)).until {
            pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)
            subscribesOf(server.receivedMessages).size >= 2
        }
    }

    @Test
    fun `구독이 확정되면 ACK 유효기간이 지나도 재등록하지 않는다`() {
        val pool = pool(ackTimeoutMillis = 100)
        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)

        server.broadcastText(ackFrame("000001", success = true))
        Thread.sleep(300)
        now += 500
        repeat(3) { pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true) }

        Thread.sleep(200)
        assertEquals(1, subscribesOf(server.receivedMessages).size)
    }

    @Test
    fun `ACK 없이 수요가 사라져도 등록 추적이 남아 해지를 보낸다`() {
        val pool = pool(ackTimeoutMillis = 100, graceMillis = 1_000)
        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)

        pool.maintain(emptySet(), emptyList(), subscribeAllowed = true)
        now += 1_500
        pool.maintain(emptySet(), emptyList(), subscribeAllowed = true)

        server.awaitMessages(2)
        val unsubscribed = unsubscribesOf(server.receivedMessages)
        assertEquals(1, unsubscribed.size)
        assertEquals("000001", unsubscribed[0].path("body").path("input").path("tr_key").asText())
    }

    @Test
    fun `ACK 유효기간이 지나 재등록한 뒤 늦게 온 ACK도 확정으로 흡수한다`() {
        val meters = SimpleMeterRegistry()
        val pool = pool(ackTimeoutMillis = 100, meters = meters)
        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)
        now += 500
        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(2)

        server.broadcastText(ackFrame("000001", success = true))

        awaitConfirmed(meters, 1)
    }

    @Test
    fun `응답이 없으면 ACK 유효기간 뒤에 재등록한다`() {
        val pool = pool(ackTimeoutMillis = 100)
        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)

        now += 500
        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)

        server.awaitMessages(2)
        assertEquals(2, subscribesOf(server.receivedMessages).size)
    }

    @Test
    fun `해지는 유예가 지난 뒤에만 전송된다`() {
        val pool = pool(graceMillis = 1_000)
        pool.maintain(linkedSetOf("000001", "000002"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(2)

        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)
        Thread.sleep(200)
        assertTrue(unsubscribesOf(server.receivedMessages).isEmpty())

        now += 1_500
        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)

        server.awaitMessages(3)
        val unsubscribed = unsubscribesOf(server.receivedMessages)
        assertEquals(1, unsubscribed.size)
        assertEquals("000002", unsubscribed[0].path("body").path("input").path("tr_key").asText())
    }

    @Test
    fun `유예 중 재수요가 오면 해지가 취소된다`() {
        val pool = pool(graceMillis = 1_000)
        pool.maintain(linkedSetOf("000001", "000002"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(2)

        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)
        pool.maintain(linkedSetOf("000001", "000002"), emptyList(), subscribeAllowed = true)
        now += 2_000
        pool.maintain(linkedSetOf("000001", "000002"), emptyList(), subscribeAllowed = true)

        Thread.sleep(200)
        assertTrue(unsubscribesOf(server.receivedMessages).isEmpty())
    }

    @Test
    fun `disconnectAll은 해지 후 연결을 닫는다`() {
        val pool = pool()
        pool.maintain(setOf("005930"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(1)

        pool.disconnectAll()

        server.awaitMessages(2)
        assertEquals(1, unsubscribesOf(server.receivedMessages).size)
        await().atMost(Duration.ofSeconds(5)).until { server.connectionCount == 0 }
    }

    @Test
    fun `TR이 여러 개면 심볼당 TR별로 모두 등록한다`() {
        val pool = pool(maxPerSession = 4, trIds = listOf("H0UNCNT0", "H0STOUP0"))

        pool.maintain(setOf("005930"), emptyList(), subscribeAllowed = true)

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

        pool.maintain(linkedSetOf("000001", "000002", "000003"), emptyList(), subscribeAllowed = true)

        server.awaitMessages(4)
        assertEquals(4, subscribesOf(server.receivedMessages).size)
        assertEquals(setOf("000003"), pool.degradedSymbols())
    }

    @Test
    fun `해지 시 심볼의 모든 TR을 해제한다`() {
        val pool = pool(maxPerSession = 4, graceMillis = 1_000, trIds = listOf("H0UNCNT0", "H0STOUP0"))
        pool.maintain(linkedSetOf("000001", "000002"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(4)

        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)
        now += 1_500
        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)

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
        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)
        server.awaitMessages(2)

        server.broadcastText(ackFrame("000001", success = true, trId = "H0UNCNT0"))
        Thread.sleep(300)
        now += 500
        pool.maintain(setOf("000001"), emptyList(), subscribeAllowed = true)

        server.awaitMessages(3)
        Thread.sleep(200)
        val resubscribed = subscribesOf(server.receivedMessages).drop(2)
        assertEquals(1, resubscribed.size)
        assertEquals("H0STOUP0", resubscribed[0].path("body").path("input").path("tr_id").asText())
    }
}
