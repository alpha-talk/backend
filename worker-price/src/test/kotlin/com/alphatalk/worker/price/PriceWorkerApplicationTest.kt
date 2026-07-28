package com.alphatalk.worker.price

import com.alphatalk.worker.price.config.PriceProperties
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import kotlin.test.assertEquals
import kotlin.test.assertFalse

@SpringBootTest(properties = ["alphatalk.price.enabled=false"])
class PriceWorkerApplicationTest {
    @Autowired
    private lateinit var properties: PriceProperties

    @Test
    fun `컨텍스트 로드`() {
    }

    @Test
    fun `기본 프로필은 비활성 상태로 뜨고 모의투자 환경을 가리킨다`() {
        assertFalse(properties.enabled)
        assertEquals("vts", properties.env)
        assertEquals(200, properties.conflationMs)
    }
}
