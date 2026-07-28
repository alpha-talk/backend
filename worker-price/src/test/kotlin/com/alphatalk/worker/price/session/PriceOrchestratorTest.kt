package com.alphatalk.worker.price.session

import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.test.FakeKisServer
import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.conflation.ConflationBuffer
import com.alphatalk.worker.price.demand.FixedDemandSource
import com.alphatalk.worker.price.leader.LeaderLock
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.awaitility.Awaitility.await
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PriceOrchestratorTest {
    private lateinit var server: FakeKisServer

    @BeforeTest
    fun setUp() {
        server = FakeKisServer()
        server.startAndAwait()
    }

    @AfterTest
    fun tearDown() {
        server.close()
    }

    private fun pool() = SessionPool(
        accounts = listOf(KisAccount("key1", "app", "secret")),
        wsUrl = server.url,
        approvalKeys = { "AK" },
        buffer = ConflationBuffer(),
        meters = SimpleMeterRegistry(),
        backoff = BackoffPolicy(initialMillis = 50, jitterRatio = 0.0),
    )

    private fun orchestrator(leader: LeaderLock, calendar: MarketCalendar, pool: SessionPool) =
        PriceOrchestrator(FixedDemandSource(listOf("005930")), pool, calendar, leader, SimpleMeterRegistry())

    private fun sundayClock(): () -> Instant =
        { ZonedDateTime.of(2026, 7, 26, 10, 0, 0, 0, ZoneId.of("Asia/Seoul")).toInstant() }

    @Test
    fun `리더가 아니면 세션을 만들지 않는다`() {
        val orchestrator = orchestrator(ToggleLeaderLock(leader = false), MarketCalendar(enforced = false), pool())

        orchestrator.tick()
        orchestrator.tick()

        Thread.sleep(200)
        assertEquals(0, server.connectionCount)
    }

    @Test
    fun `리더면 수요 종목을 구독한다`() {
        val orchestrator = orchestrator(ToggleLeaderLock(leader = true), MarketCalendar(enforced = false), pool())

        orchestrator.tick()

        server.awaitConnections(1)
        server.awaitMessages(1)
    }

    @Test
    fun `장이 닫혀 있으면 연결하지 않는다`() {
        val calendar = MarketCalendar(clock = sundayClock())
        val orchestrator = orchestrator(ToggleLeaderLock(leader = true), calendar, pool())

        orchestrator.tick()

        Thread.sleep(200)
        assertEquals(0, server.connectionCount)
    }

    @Test
    fun `shutdown은 세션을 닫고 리더 락을 놓는다`() {
        val leader = ToggleLeaderLock(leader = true)
        val orchestrator = orchestrator(leader, MarketCalendar(enforced = false), pool())
        orchestrator.tick()
        server.awaitConnections(1)

        orchestrator.shutdown()

        await().atMost(Duration.ofSeconds(5)).until { server.connectionCount == 0 }
        assertTrue(leader.released)
    }

    private class ToggleLeaderLock(var leader: Boolean) : LeaderLock {
        var released = false

        override fun tryAcquire(): Boolean = leader

        override fun release() {
            released = true
        }
    }
}
