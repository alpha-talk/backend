package com.alphatalk.worker.batch.job

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class StartupCatchUpTest {
    private val meters = SimpleMeterRegistry()
    private val seoul = ZoneId.of("Asia/Seoul")

    private fun at(text: String): ZonedDateTime = ZonedDateTime.parse("${text}+09:00[Asia/Seoul]")

    private fun task(name: String, cron: String, ran: MutableList<String>) =
        CatchUpTask(name, cron) { ran += name }

    @Test
    fun `오늘 실행 시각이 이미 지났으면 잡을 돌린다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            tasks = listOf(task("valuation_daily", "0 50 16 * * MON-FRI", ran)),
            meters = meters,
            now = { at("2026-08-07T18:30:00") },
        )

        assertEquals(listOf("valuation_daily"), catchUp.runPending().succeeded)
        assertEquals(listOf("valuation_daily"), ran)
        assertEquals(1.0, meters.counter("batch.catchup.triggered", "job", "valuation_daily").count())
    }

    @Test
    fun `자정 크론도 오늘 창으로 인정한다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            tasks = listOf(task("midnight_job", "0 0 0 * * *", ran)),
            meters = meters,
            now = { at("2026-08-07T09:00:00") },
        )

        assertEquals(listOf("midnight_job"), catchUp.runPending().succeeded)
    }

    @Test
    fun `실행 시각 전에 뜬 기동은 돌리지 않는다 - 정규 스케줄이 곧 쏜다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            tasks = listOf(task("valuation_daily", "0 50 16 * * MON-FRI", ran)),
            meters = meters,
            now = { at("2026-08-07T10:00:00") },
        )

        assertTrue(catchUp.runPending().succeeded.isEmpty())
        assertTrue(ran.isEmpty())
    }

    @Test
    fun `오늘 실행 예정이 없는 날은 돌리지 않는다 - 주말`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            tasks = listOf(task("valuation_daily", "0 50 16 * * MON-FRI", ran)),
            meters = meters,
            now = { at("2026-08-08T23:00:00") },
        )

        assertTrue(catchUp.runPending().succeeded.isEmpty())
        assertTrue(ran.isEmpty())
    }

    @Test
    fun `등록 순서대로 실행한다 - 마스터가 먼저여야 뒤 잡이 빈 유니버스를 안 본다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            tasks = listOf(
                task("stock_master_sync", "0 0 8 * * *", ran),
                task("valuation_daily", "0 50 16 * * MON-FRI", ran),
                task("investor_flow_daily", "0 10 17 * * MON-FRI", ran),
                task("financials_sync", "0 0 6 * * *", ran),
            ),
            meters = meters,
            now = { at("2026-08-07T19:00:00") },
        )

        val expected = listOf("stock_master_sync", "valuation_daily", "investor_flow_daily", "financials_sync")
        assertEquals(expected, catchUp.runPending().succeeded)
        assertEquals(expected, ran)
    }

    @Test
    fun `창이 지난 잡만 골라 돌린다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            tasks = listOf(
                task("financials_sync", "0 0 6 * * *", ran),
                task("valuation_daily", "0 50 16 * * MON-FRI", ran),
            ),
            meters = meters,
            now = { at("2026-08-07T09:00:00") },
        )

        assertEquals(listOf("financials_sync"), catchUp.runPending().succeeded)
    }

    @Test
    fun `한 잡이 실패해도 다음 잡을 계속 돌린다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            tasks = listOf(
                CatchUpTask("stock_master_sync", "0 0 8 * * *") { throw IllegalStateException("boom") },
                task("valuation_daily", "0 50 16 * * MON-FRI", ran),
            ),
            meters = meters,
            now = { at("2026-08-07T19:00:00") },
        )

        assertEquals(listOf("valuation_daily"), catchUp.runPending().succeeded)
        assertEquals(listOf("valuation_daily"), ran)
        assertEquals(1.0, meters.counter("batch.catchup.failed", "job", "stock_master_sync").count())
    }

    @Test
    fun `스케줄이 꺼진 잡은 따라잡지 않는다 - cron 비활성 값`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            tasks = listOf(task("stock_master_sync", "-", ran)),
            meters = meters,
            now = { at("2026-08-07T19:00:00") },
        )

        assertTrue(catchUp.runPending().succeeded.isEmpty())
        assertTrue(ran.isEmpty())
        assertEquals(0.0, meters.counter("batch.catchup.failed", "job", "stock_master_sync").count())
    }

    @Test
    fun `앞 잡이 자정을 넘기면 남은 잡은 새 날짜로 다시 판정한다`() {
        val ran = mutableListOf<String>()
        var current = at("2026-08-07T23:50:00")
        val catchUp = StartupCatchUp(
            tasks = listOf(
                CatchUpTask("stock_master_sync", "0 0 8 * * *") {
                    ran += "stock_master_sync"
                    current = at("2026-08-08T00:10:00")
                },
                task("valuation_daily", "0 50 16 * * MON-FRI", ran),
            ),
            meters = meters,
            now = { current },
        )

        assertEquals(listOf("stock_master_sync"), catchUp.runPending().succeeded)
        assertEquals(listOf("stock_master_sync"), ran)
    }

    @Test
    fun `등록된 잡이 없으면 기동해도 아무 일도 하지 않는다`() {
        val catchUp = StartupCatchUp(tasks = emptyList(), meters = meters, now = { at("2026-08-07T19:00:00") })

        catchUp.onApplicationReady()

        assertTrue(catchUp.runPending().succeeded.isEmpty())
    }

    @Test
    fun `스테일 락에 막혔을 수 있으니 간격을 두고 여러 번 다시 확인한다`() {
        val ran = mutableListOf<String>()
        val slept = mutableListOf<Duration>()
        val catchUp = StartupCatchUp(
            tasks = listOf(task("valuation_daily", "0 50 16 * * MON-FRI", ran)),
            meters = meters,
            passes = 3,
            passInterval = Duration.ofMinutes(20),
            now = { at("2026-08-07T19:00:00") },
            sleep = { slept += it },
        )

        catchUp.runPasses()

        assertEquals(3, ran.size)
        assertEquals(Duration.ofMinutes(40), slept.fold(Duration.ZERO, Duration::plus))
    }

    @Test
    fun `종료 플래그가 서면 남은 대기를 즉시 끊는다 - 인터럽트를 쏘지 않는다`() {
        val ran = mutableListOf<String>()
        val slept = mutableListOf<Duration>()
        lateinit var catchUp: StartupCatchUp
        catchUp = StartupCatchUp(
            tasks = listOf(task("valuation_daily", "0 50 16 * * MON-FRI", ran)),
            meters = meters,
            passes = 3,
            passInterval = Duration.ofMinutes(20),
            now = { at("2026-08-07T19:00:00") },
            sleep = {
                slept += it
                catchUp.destroy()
            },
        )

        catchUp.runPasses()

        assertEquals(1, ran.size)
        assertEquals(1, slept.size)
        assertTrue(slept.single() < Duration.ofMinutes(20))
        assertTrue(Thread.interrupted().not())
    }

    @Test
    fun `첫 패스에서 돌릴 잡이 없으면 다시 확인하지 않는다`() {
        val ran = mutableListOf<String>()
        val slept = mutableListOf<Duration>()
        val catchUp = StartupCatchUp(
            tasks = listOf(task("valuation_daily", "0 50 16 * * MON-FRI", ran)),
            meters = meters,
            now = { at("2026-08-07T10:00:00") },
            sleep = { slept += it },
        )

        catchUp.runPasses()

        assertTrue(ran.isEmpty())
        assertTrue(slept.isEmpty())
    }

    @Test
    fun `종료가 시작되면 남은 잡을 새로 시작하지 않는다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            tasks = listOf(
                CatchUpTask("stock_master_sync", "0 0 8 * * *") { ran += "stock_master_sync" },
                task("valuation_daily", "0 50 16 * * MON-FRI", ran),
            ),
            meters = meters,
            now = { at("2026-08-07T19:00:00") },
        )
        catchUp.destroy()

        assertTrue(catchUp.runPending().succeeded.isEmpty())
        assertTrue(ran.isEmpty())
    }

    @Test
    fun `stop-timeout이 0이어도 종료가 무한 대기하지 않는다`() {
        val started = java.util.concurrent.CountDownLatch(1)
        val catchUp = StartupCatchUp(
            tasks = listOf(
                CatchUpTask("stock_master_sync", "0 0 8 * * *") {
                    started.countDown()
                    Thread.sleep(60_000)
                },
            ),
            meters = meters,
            stopTimeout = Duration.ZERO,
            now = { at("2026-08-07T19:00:00") },
        )

        catchUp.onApplicationReady()
        assertTrue(started.await(5, java.util.concurrent.TimeUnit.SECONDS))

        catchUp.destroy()
    }

    @Test
    fun `첫 패스가 전부 실패해도 다시 확인한다 - 아직 안 온 선행 잡의 창이 그 사이 열린다`() {
        val attempts = mutableListOf<String>()
        val slept = mutableListOf<Duration>()
        val catchUp = StartupCatchUp(
            tasks = listOf(
                CatchUpTask("financials_sync", "0 0 6 * * *") {
                    attempts += "financials_sync"
                    throw IllegalStateException("stock_master is empty")
                },
            ),
            meters = meters,
            passes = 3,
            now = { at("2026-08-07T07:30:00") },
            sleep = { slept += it },
        )

        catchUp.runPasses()

        assertEquals(3, attempts.size)
        assertEquals(Duration.ofMinutes(40), slept.fold(Duration.ZERO, Duration::plus))
    }

    @Test
    fun `잡 실행 중 인터럽트되면 다음 패스를 기다리지 않고 즉시 끝낸다`() {
        val ran = mutableListOf<String>()
        val slept = mutableListOf<Duration>()
        val catchUp = StartupCatchUp(
            tasks = listOf(
                CatchUpTask("valuation_daily", "0 50 16 * * MON-FRI") {
                    ran += "valuation_daily"
                    throw InterruptedException("shutdown")
                },
            ),
            meters = meters,
            passes = 3,
            now = { at("2026-08-07T19:00:00") },
            sleep = { slept += it },
        )

        catchUp.runPasses()

        assertEquals(1, ran.size)
        assertTrue(slept.isEmpty())
        assertTrue(Thread.interrupted())
    }

    @Test
    fun `잘못된 재확인 설정은 기동에서 막는다`() {
        assertFailsWith<IllegalArgumentException> {
            StartupCatchUp(tasks = emptyList(), meters = meters, passes = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            StartupCatchUp(tasks = emptyList(), meters = meters, passInterval = Duration.ofMinutes(-1))
        }
    }

    @Test
    fun `종료 중에는 다음 패스를 기다리지 않는다`() {
        val ran = mutableListOf<String>()
        val catchUp = StartupCatchUp(
            tasks = listOf(task("valuation_daily", "0 50 16 * * MON-FRI", ran)),
            meters = meters,
            passes = 3,
            now = { at("2026-08-07T19:00:00") },
            sleep = { throw InterruptedException("shutdown") },
        )

        catchUp.runPasses()

        assertEquals(1, ran.size)
        assertTrue(Thread.interrupted())
    }
}
