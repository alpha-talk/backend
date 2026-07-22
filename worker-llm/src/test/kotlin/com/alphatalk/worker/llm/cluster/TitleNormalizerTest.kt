package com.alphatalk.worker.llm.cluster

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class TitleNormalizerTest {
    @Test
    fun `속보 태그·특수문자 제거 후 동일 해시`() {
        val a = TitleNormalizer.hash("[속보] 삼성전자, 3나노 대규모 수주!")
        val b = TitleNormalizer.hash("삼성전자 3나노 대규모 수주")
        assertEquals(a, b)
    }

    @Test
    fun `다른 사건은 다른 해시`() {
        assertNotEquals(
            TitleNormalizer.hash("삼성전자 3나노 수주"),
            TitleNormalizer.hash("삼성전자 리콜 발표"),
        )
    }

    @Test
    fun `해시 길이 16`() {
        assertEquals(16, TitleNormalizer.hash("제목").length)
    }
}
