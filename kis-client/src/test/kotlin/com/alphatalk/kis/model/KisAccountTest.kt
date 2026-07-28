package com.alphatalk.kis.model

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KisAccountTest {
    @Test
    fun `toString은 시크릿을 노출하지 않는다`() {
        val printed = KisAccount("key1", "app-key-secret", "app-secret-value").toString()

        assertTrue("key1" in printed)
        assertFalse("app-key-secret" in printed)
        assertFalse("app-secret-value" in printed)
    }
}
