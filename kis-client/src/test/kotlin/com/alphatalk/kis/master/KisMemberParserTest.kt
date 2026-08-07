package com.alphatalk.kis.master

import java.nio.charset.Charset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KisMemberParserTest {
    private val cp949 = Charset.forName("x-windows-949")

    private fun master(vararg lines: String): ByteArray =
        lines.joinToString("\n").toByteArray(cp949)

    @Test
    fun `회원사 코드·이름·외국계 플래그를 파싱한다`() {
        val parsed = KisMemberParser.parse(
            master(
                "99999외국계합            1",
                "00017KB증권             0",
                "00033JP모간             1",
                "00003한국증권            0",
            ),
        )

        assertEquals(0, parsed.skippedLines)
        assertEquals(4, parsed.members.size)
        val kb = parsed.members.single { it.code == "00017" }
        assertEquals("KB증권", kb.name)
        assertFalse(kb.foreign)
        assertTrue(parsed.members.single { it.code == "00033" }.foreign)
    }

    @Test
    fun `요청 코드는 5자리의 뒤 3자리다`() {
        val member = KisMember(code = "00003", name = "한국증권", foreign = false)
        assertEquals("003", member.queryCode)
    }

    @Test
    fun `외국계합 집계 행을 식별한다`() {
        val parsed = KisMemberParser.parse(master("99999외국계합            1", "00017KB증권             0"))
        assertEquals(listOf("99999"), parsed.members.filter { it.aggregate }.map { it.code })
    }

    @Test
    fun `형식이 깨진 줄은 건너뛰고 개수를 센다`() {
        val parsed = KisMemberParser.parse(master("00017KB증권             0", "잘못된줄", "x", ""))
        assertEquals(1, parsed.members.size)
        assertEquals(2, parsed.skippedLines)
    }

    @Test
    fun `CRLF 줄바꿈을 처리한다`() {
        val parsed = KisMemberParser.parse("00017KB증권             0\r\n00003한국증권            0\r\n".toByteArray(cp949))
        assertEquals(listOf("00017", "00003"), parsed.members.map { it.code })
    }
}
