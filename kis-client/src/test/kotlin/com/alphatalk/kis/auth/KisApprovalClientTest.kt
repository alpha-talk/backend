package com.alphatalk.kis.auth

import com.alphatalk.kis.KisClientException
import com.alphatalk.kis.RecordingKisServer
import com.alphatalk.kis.model.KisAccount
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KisApprovalClientTest {
    private lateinit var server: RecordingKisServer
    private val account = KisAccount("key1", "app-key", "app-secret")

    @BeforeTest
    fun setUp() {
        server = RecordingKisServer()
    }

    @AfterTest
    fun tearDown() {
        server.close()
    }

    @Test
    fun `approval 요청 본문은 secretkey 필드를 쓴다`() {
        server.enqueue("/oauth2/Approval", 200, """{"approval_key":"AK-123"}""")
        val client = KisApprovalClient(server.baseUrl)

        assertEquals("AK-123", client.approvalKey(account))

        val body = server.received.single { it.path == "/oauth2/Approval" }.body
        assertTrue("\"secretkey\":\"app-secret\"" in body)
        assertTrue("\"appkey\":\"app-key\"" in body)
        assertTrue("\"grant_type\":\"client_credentials\"" in body)
        assertFalse("appsecret" in body)
    }

    @Test
    fun `approval_key가 없으면 예외를 던진다`() {
        server.enqueue("/oauth2/Approval", 200, "{}")
        val client = KisApprovalClient(server.baseUrl)

        assertFailsWith<KisClientException> { client.approvalKey(account) }
    }
}
