package com.alphatalk.coreapi.community

import com.alphatalk.contracts.Channels
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
class CommunityEndToEndTest {
    companion object {
        private const val PROBE = "probe"

        @Container
        @ServiceConnection
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)

        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )
    }

    @Autowired
    private lateinit var rest: TestRestTemplate

    @Autowired
    private lateinit var jdbc: JdbcTemplate

    @Autowired
    private lateinit var redisTemplate: StringRedisTemplate

    @Autowired
    private lateinit var connectionFactory: RedisConnectionFactory

    private val mapper = ObjectMapper()
    private val published = LinkedBlockingQueue<String>()
    private lateinit var listener: RedisMessageListenerContainer
    private lateinit var authorToken: String
    private lateinit var readerToken: String
    private var authorId = 0L

    @BeforeEach
    fun seed() {
        redisTemplate.connectionFactory!!.connection.use { it.serverCommands().flushDb() }
        jdbc.update("DELETE FROM report")
        jdbc.update("DELETE FROM post_like")
        jdbc.update("DELETE FROM comment")
        jdbc.update("DELETE FROM post")
        jdbc.update("DELETE FROM stream_event")
        jdbc.update("DELETE FROM refresh_tokens")
        jdbc.update("DELETE FROM users")
        jdbc.update("DELETE FROM stock_master")
        jdbc.update(
            """
            INSERT INTO stock_master (code, name, market, shares_outstanding, is_active) VALUES
            ('005930', '삼성전자', 'KOSPI', 5846278000, true)
            """.trimIndent(),
        )
        jdbc.update(
            """
            INSERT INTO stream_event (event_id, code, type, occurred_at, source, payload)
            VALUES ('01J9Z800000000000000000001', '005930', 'NEWS', now(), 'hankyung', '{"title":"인용 뉴스"}'::jsonb)
            """.trimIndent(),
        )
        authorToken = signup("author@b.c", "글쓴이")
        readerToken = signup("reader@b.c", "독자")
        authorId = jdbc.queryForObject("SELECT id FROM users WHERE email = 'author@b.c'", Long::class.java)!!
        startListening()
    }

    @AfterEach
    fun stopListening() {
        listener.stop()
        listener.destroy()
    }

    private fun signup(email: String, nickname: String): String {
        post("/api/v1/auth/signup", """{"email":"$email","password":"password1","nickname":"$nickname"}""")
        return json(post("/api/v1/auth/login", """{"email":"$email","password":"password1"}""").body)
            .path("accessToken").asText()
    }

    private fun startListening() {
        listener = RedisMessageListenerContainer()
        listener.setConnectionFactory(connectionFactory)
        listener.afterPropertiesSet()
        listener.start()
        listener.addMessageListener(
            { message, _ -> published += String(message.body) },
            ChannelTopic(Channels.post("005930")),
        )
        awaitSubscription()
    }

    private fun awaitSubscription() {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (System.nanoTime() < deadline) {
            redisTemplate.convertAndSend(Channels.post("005930"), PROBE)
            val received = published.poll(200, TimeUnit.MILLISECONDS)
            if (received == PROBE) {
                published.clear()
                return
            }
        }
        error("Redis 구독이 준비되지 않았다")
    }

    private fun awaitPublished(): JsonNode {
        while (true) {
            val message = published.poll(5, TimeUnit.SECONDS) ?: error("post 채널 발행을 받지 못했다")
            if (message != PROBE) return mapper.readTree(message)
        }
    }

    private fun json(body: String?): JsonNode = mapper.readTree(body)

    private fun exchange(
        method: HttpMethod,
        path: String,
        body: String?,
        bearer: String?,
        headers: Map<String, String> = emptyMap(),
    ) = rest.exchange(
        path,
        method,
        HttpEntity(
            body,
            HttpHeaders().apply {
                if (body != null) contentType = MediaType.APPLICATION_JSON
                bearer?.let { set("Authorization", "Bearer $it") }
                headers.forEach { (name, value) -> set(name, value) }
            },
        ),
        String::class.java,
    )

    private fun post(path: String, body: String? = null, bearer: String? = null, headers: Map<String, String> = emptyMap()) =
        exchange(HttpMethod.POST, path, body, bearer, headers)

    private fun get(path: String, bearer: String? = authorToken) = exchange(HttpMethod.GET, path, null, bearer)

    private fun createPost(
        content: String = "본문입니다",
        quoted: String? = "01J9Z800000000000000000001",
        idempotencyKey: String? = null,
    ): JsonNode {
        val quotedField = quoted?.let { ""","quotedEventId":"$it"""" } ?: ""
        val response = post(
            "/api/v1/rooms/005930/posts",
            """{"title":"2분기 실적 감상","content":"$content"$quotedField}""",
            authorToken,
            idempotencyKey?.let { mapOf("Idempotency-Key" to it) } ?: emptyMap(),
        )
        assertEquals(201, response.statusCode.value(), response.body ?: "")
        return json(response.body)
    }

    @Test
    fun `FR-09 - 글을 쓰면 한 트랜잭션의 더블라이트 뒤 post 채널로 발행된다`() {
        val created = createPost()
        val postId = created.path("postId").asText()
        assertEquals(postId, created.path("eventId").asText())

        val row = jdbc.queryForMap("SELECT type, code, payload FROM stream_event WHERE event_id = ?", postId)
        assertEquals("POST", row["type"])
        val payload = mapper.readTree(row["payload"].toString())
        assertEquals("post", payload.path("kind").asText())
        assertEquals("글쓴이", payload.path("author").path("nickname").asText())
        assertEquals("본문입니다", payload.path("preview").asText())

        val envelope = awaitPublished()
        assertEquals("post", envelope.path("type").asText())
        assertEquals("005930", envelope.path("code").asText())
        assertEquals(postId, envelope.path("eventId").asText())
        assertEquals("post", envelope.path("data").path("kind").asText())
        assertEquals("본문입니다", envelope.path("data").path("content").asText())

        val stream = json(get("/api/v1/rooms/005930/stream?types=post").body)
        assertEquals(postId, stream.path("items")[0].path("eventId").asText())
    }

    @Test
    fun `같은 Idempotency-Key로 다시 보내면 같은 응답이고 글은 하나다`() {
        val key = "01JA00000000000000000000AA"
        val first = createPost(idempotencyKey = key)
        val replay = createPost(idempotencyKey = key)

        assertEquals(first.path("postId").asText(), replay.path("postId").asText())
        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM post", Long::class.java))
    }

    @Test
    fun `FR-10 - 다른 방의 이벤트는 인용할 수 없다`() {
        val response = post(
            "/api/v1/rooms/005930/posts",
            """{"title":"제목","content":"본문","quotedEventId":"01J9Z800000000000000000099"}""",
            authorToken,
        )

        assertEquals(400, response.statusCode.value())
        assertEquals("VALIDATION_FAILED", json(response.body).path("error").path("code").asText())
    }

    @Test
    fun `댓글을 달면 카운트가 오르고 comment 봉투가 발행된다`() {
        val postId = createPost().path("postId").asText()
        awaitPublished()

        val comment = post("/api/v1/posts/$postId/comments", """{"content":"첫 댓글"}""", readerToken)
        assertEquals(201, comment.statusCode.value())
        val commentId = json(comment.body).path("commentId").asText()

        val envelope = awaitPublished()
        assertEquals("comment", envelope.path("data").path("kind").asText())
        assertEquals(commentId, envelope.path("eventId").asText())
        assertEquals(postId, envelope.path("data").path("parentId").asText())

        val detail = json(get("/api/v1/posts/$postId").body)
        assertEquals(1, detail.path("commentCount").asInt())
        assertEquals("첫 댓글", detail.path("comments").path("items")[0].path("content").asText())
        assertEquals("독자", detail.path("comments").path("items")[0].path("author").path("nickname").asText())
    }

    @Test
    fun `공감은 멱등하고 취소하면 되돌아간다`() {
        val postId = createPost().path("postId").asText()

        assertEquals(204, exchange(HttpMethod.PUT, "/api/v1/posts/$postId/like", null, readerToken).statusCode.value())
        assertEquals(204, exchange(HttpMethod.PUT, "/api/v1/posts/$postId/like", null, readerToken).statusCode.value())

        val liked = json(get("/api/v1/posts/$postId", bearer = readerToken).body)
        assertEquals(1, liked.path("likeCount").asInt())
        assertEquals(true, liked.path("likedByMe").asBoolean())

        assertEquals(204, exchange(HttpMethod.DELETE, "/api/v1/posts/$postId/like", null, readerToken).statusCode.value())
        val unliked = json(get("/api/v1/posts/$postId", bearer = readerToken).body)
        assertEquals(0, unliked.path("likeCount").asInt())
        assertEquals(false, unliked.path("likedByMe").asBoolean())
    }

    @Test
    fun `글 상세에는 인용 이벤트 원문이 붙는다`() {
        val postId = createPost().path("postId").asText()

        val detail = json(get("/api/v1/posts/$postId").body)

        assertEquals("01J9Z800000000000000000001", detail.path("quoted").path("eventId").asText())
        assertEquals("NEWS", detail.path("quoted").path("type").asText())
        assertEquals("인용 뉴스", detail.path("quoted").path("payload").path("title").asText())
    }

    @Test
    fun `작성자만 수정할 수 있다`() {
        val postId = createPost().path("postId").asText()

        val forbidden = exchange(HttpMethod.PATCH, "/api/v1/posts/$postId", """{"title":"해킹"}""", readerToken)
        assertEquals(403, forbidden.statusCode.value())

        val updated = exchange(HttpMethod.PATCH, "/api/v1/posts/$postId", """{"title":"고친 제목"}""", authorToken)
        assertEquals(200, updated.statusCode.value())
        assertEquals("고친 제목", json(updated.body).path("title").asText())
        assertNotNull(json(updated.body).path("updatedAt").takeIf { !it.isNull })
    }

    @Test
    fun `소프트 삭제하면 스트림 이벤트에 deleted가 마킹된다`() {
        val postId = createPost().path("postId").asText()

        val deleted = exchange(HttpMethod.DELETE, "/api/v1/posts/$postId", null, authorToken)
        assertEquals(204, deleted.statusCode.value())

        val payload = mapper.readTree(
            jdbc.queryForObject("SELECT payload FROM stream_event WHERE event_id = ?", String::class.java, postId),
        )
        assertEquals(true, payload.path("deleted").asBoolean())

        val detail = json(get("/api/v1/posts/$postId").body)
        assertEquals(true, detail.path("deleted").asBoolean())
        assertEquals("", detail.path("content").asText())

        val list = json(get("/api/v1/rooms/005930/posts").body)
        assertEquals(0, list.path("items").size())
        assertNotNull(jdbc.queryForObject("SELECT deleted_at FROM post WHERE id = ?", java.sql.Timestamp::class.java, postId))
    }

    @Test
    fun `댓글 삭제는 작성자만 가능하고 카운트를 되돌린다`() {
        val postId = createPost().path("postId").asText()
        val commentId = json(
            post("/api/v1/posts/$postId/comments", """{"content":"지울 댓글"}""", readerToken).body,
        ).path("commentId").asText()

        val forbidden = exchange(HttpMethod.DELETE, "/api/v1/comments/$commentId", null, authorToken)
        assertEquals(403, forbidden.statusCode.value())

        val deleted = exchange(HttpMethod.DELETE, "/api/v1/comments/$commentId", null, readerToken)
        assertEquals(204, deleted.statusCode.value())

        val detail = json(get("/api/v1/posts/$postId").body)
        assertEquals(0, detail.path("commentCount").asInt())
        assertEquals(0, detail.path("comments").path("items").size())
    }

    @Test
    fun `FR-18 - 신고가 접수되어 저장된다`() {
        val postId = createPost().path("postId").asText()

        val response = post(
            "/api/v1/posts/$postId/report",
            """{"reason":"SPAM","detail":"도배 글"}""",
            readerToken,
        )

        assertEquals(201, response.statusCode.value())
        val row = jdbc.queryForMap("SELECT target_type, target_id, reason, status FROM report")
        assertEquals("POST", row["target_type"])
        assertEquals("SPAM", row["reason"])
        assertEquals("RECEIVED", row["status"])
    }

    @Test
    fun `FR-19 - 글 작성은 분당 5회를 넘기면 429와 Retry-After를 준다`() {
        repeat(5) { createPost(content = "본문 $it", quoted = null) }

        val throttled = post(
            "/api/v1/rooms/005930/posts",
            """{"title":"제목","content":"여섯 번째"}""",
            authorToken,
        )

        assertEquals(429, throttled.statusCode.value())
        assertEquals("RATE_LIMITED", json(throttled.body).path("error").path("code").asText())
        assertTrue((throttled.headers.getFirst("Retry-After")?.toLong() ?: 0) in 1..60)
    }

    @Test
    fun `방 글 목록은 최신부터 커서로 넘긴다`() {
        val ids = (1..3).map { createPost(content = "본문 $it", quoted = null).path("postId").asText() }

        val first = json(get("/api/v1/rooms/005930/posts?limit=2").body)
        assertEquals(listOf(ids[2], ids[1]), first.path("items").map { it.path("postId").asText() })
        assertEquals(true, first.path("pageInfo").path("hasMoreBefore").asBoolean())

        val next = json(get("/api/v1/rooms/005930/posts?limit=2&cursor=${ids[1]}").body)
        assertEquals(listOf(ids[0]), next.path("items").map { it.path("postId").asText() })
        assertEquals(false, next.path("pageInfo").path("hasMoreBefore").asBoolean())
    }

    @Test
    fun `커뮤니티 API는 인증이 필요하다`() {
        assertEquals(401, post("/api/v1/rooms/005930/posts", """{"title":"t","content":"c"}""").statusCode.value())
        assertEquals(401, get("/api/v1/rooms/005930/posts", bearer = null).statusCode.value())
    }
}
