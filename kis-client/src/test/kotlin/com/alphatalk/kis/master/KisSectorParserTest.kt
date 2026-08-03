package com.alphatalk.kis.master

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KisSectorParserTest {
    private val parsed by lazy {
        KisSectorParser.parse(javaClass.getResourceAsStream("/fixtures/idxcode_sample.mst")!!.readBytes())
    }

    @Test
    fun `업종 코드와 이름을 읽는다`() {
        assertEquals("제조", parsed.sectors.single { it.code == "00027" }.name)
        assertEquals("IT 서비스", parsed.sectors.single { it.code == "00029" }.name)
    }

    @Test
    fun `KOSPI와 KOSDAQ 업종이 각자의 코드 대역을 쓴다`() {
        assertTrue(parsed.sectors.any { it.code == "00027" })
        assertEquals("제조", parsed.sectors.single { it.code == "11009" }.name)
    }

    @Test
    fun `표본이 온전하면 건너뛴 행이 없다고 보고한다`() {
        assertEquals(7, parsed.sectors.size)
        assertEquals(0, parsed.skippedLines)
        assertTrue(parsed.isComplete)
    }

    @Test
    fun `이름이 빈 코드 행은 손상이 아니라 채택 대상에서만 빠진다`() {
        assertTrue(parsed.sectors.none { it.code == "99999" }, "이름 없는 코드가 업종으로 저장됐다")
        assertEquals(0, parsed.skippedLines, "정상 파일을 손상으로 판정했다")
        assertTrue(parsed.isComplete)
    }

    @Test
    fun `이름이 빈 행 뒤의 업종도 계속 읽는다`() {
        assertEquals("ETN", parsed.sectors.single { it.code == "EE199" }.name)
    }

    @Test
    fun `읽지 못한 행은 조용히 버리지 않고 개수를 보고한다`() {
        val broken = javaClass.getResourceAsStream("/fixtures/idxcode_sample.mst")!!.readBytes() +
            "짧은 줄\n".toByteArray()

        val result = KisSectorParser.parse(broken)

        assertEquals(7, result.sectors.size)
        assertEquals(1, result.skippedLines)
        assertFalse(result.isComplete)
    }

    @Test
    fun `코드가 빈 행은 손상으로 센다`() {
        val blankCode = ByteArray(45) { ' '.code.toByte() } + '\n'.code.toByte()

        val result = KisSectorParser.parse(blankCode)

        assertEquals(1, result.skippedLines)
        assertFalse(result.isComplete)
    }

    @Test
    fun `빈 줄은 누락으로 세지 않는다`() {
        val withBlankLines = javaClass.getResourceAsStream("/fixtures/idxcode_sample.mst")!!.readBytes() +
            "\n\n".toByteArray()

        val result = KisSectorParser.parse(withBlankLines)

        assertEquals(0, result.skippedLines)
        assertTrue(result.isComplete)
    }
}
