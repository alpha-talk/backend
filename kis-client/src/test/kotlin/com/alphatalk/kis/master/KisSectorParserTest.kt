package com.alphatalk.kis.master

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KisSectorParserTest {
    private val sectors by lazy {
        KisSectorParser.parse(javaClass.getResourceAsStream("/fixtures/idxcode_sample.mst")!!.readBytes())
    }

    @Test
    fun `업종 코드와 이름을 읽는다`() {
        assertEquals("제조", sectors.single { it.code == "00027" }.name)
        assertEquals("IT 서비스", sectors.single { it.code == "00029" }.name)
    }

    @Test
    fun `KOSPI와 KOSDAQ 업종이 각자의 코드 대역을 쓴다`() {
        assertTrue(sectors.any { it.code == "00027" })
        assertTrue(sectors.any { it.code == "11009" })
        assertEquals("제조", sectors.single { it.code == "11009" }.name)
    }

    @Test
    fun `형식이 어긋난 줄은 건너뛴다`() {
        val broken = "짧은 줄\n".toByteArray()

        assertTrue(KisSectorParser.parse(broken).isEmpty())
    }

    @Test
    fun `표본을 빠짐없이 읽는다`() {
        assertEquals(6, sectors.size)
        assertTrue(sectors.all { it.code.isNotEmpty() && it.name.isNotEmpty() })
    }
}
