package com.alphatalk.worker.batch.opinion

import java.nio.charset.Charset
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class KisMemberBrokerDirectoryTest {
    private val cp949 = Charset.forName("x-windows-949")
    private val master = "99999외국계합            1\n00005미래에셋            0\n00003한국증권            0\n".toByteArray(cp949)

    private fun master(vararg lines: String): ByteArray =
        (lines.joinToString("\n") + "\n").toByteArray(cp949)

    @Test
    fun `집계 행을 제외하고 회원사를 돌려준다`() {
        val directory = KisMemberBrokerDirectory(download = { master }, today = { LocalDate.of(2026, 8, 7) })

        assertEquals(listOf("00005", "00003"), directory.brokers().map { it.code })
        assertEquals(listOf("005", "003"), directory.brokers().map { it.queryCode })
    }

    @Test
    fun `같은 날은 다시 다운로드하지 않고, 날이 바뀌면 갱신한다`() {
        var downloads = 0
        var today = LocalDate.of(2026, 8, 7)
        val directory = KisMemberBrokerDirectory(
            download = {
                downloads++
                master
            },
            today = { today },
        )

        directory.brokers()
        directory.brokers()
        assertEquals(1, downloads)

        today = LocalDate.of(2026, 8, 8)
        directory.brokers()
        assertEquals(2, downloads)
    }

    @Test
    fun `갱신 실패 시 직전 목록으로 계속 간다`() {
        var fail = false
        var today = LocalDate.of(2026, 8, 7)
        val directory = KisMemberBrokerDirectory(
            download = {
                if (fail) error("download failed")
                master
            },
            today = { today },
        )
        directory.brokers()

        fail = true
        today = LocalDate.of(2026, 8, 8)
        assertEquals(listOf("00005", "00003"), directory.brokers().map { it.code })
    }

    @Test
    fun `부분 파싱된 마스터는 직전 완전한 목록을 유지한다`() {
        var today = LocalDate.of(2026, 8, 7)
        var content = master
        val directory = KisMemberBrokerDirectory(download = { content }, today = { today })
        directory.brokers()

        today = LocalDate.of(2026, 8, 8)
        content = master("00005미래에셋            0", "깨진줄")
        assertEquals(listOf("00005", "00003"), directory.brokers().map { it.code })
    }

    @Test
    fun `내용이 비어 돌아와도 직전 목록을 유지한다`() {
        var today = LocalDate.of(2026, 8, 7)
        var content = master
        val directory = KisMemberBrokerDirectory(download = { content }, today = { today })
        directory.brokers()

        today = LocalDate.of(2026, 8, 8)
        content = master("99999외국계합            1")
        assertEquals(listOf("00005", "00003"), directory.brokers().map { it.code })
    }

    @Test
    fun `직전 목록이 없으면 부분 마스터라도 쓰되 캐시하지 않아 다음 회차가 다시 받는다`() {
        var downloads = 0
        var content = master("00005미래에셋            0", "깨진줄")
        val directory = KisMemberBrokerDirectory(
            download = {
                downloads++
                content
            },
            today = { LocalDate.of(2026, 8, 7) },
        )

        assertEquals(listOf("00005"), directory.brokers().map { it.code })
        assertEquals(1, downloads)

        assertEquals(listOf("00005"), directory.brokers().map { it.code })
        assertEquals(2, downloads)

        content = master
        assertEquals(listOf("00005", "00003"), directory.brokers().map { it.code })
        directory.brokers()
        assertEquals(3, downloads)
    }

    @Test
    fun `직전 목록조차 없으면 실패한다 - 조회 창이 직전 영업일을 포함해 하루 안 복구된다`() {
        val directory = KisMemberBrokerDirectory(download = { error("down") }, today = { LocalDate.of(2026, 8, 7) })

        assertFailsWith<IllegalStateException> { directory.brokers() }
    }
}
