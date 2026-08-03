package com.alphatalk.coreapi

import com.alphatalk.coreapi.community.CommunityService
import com.alphatalk.coreapi.community.CreatePostRequest
import com.alphatalk.coreapi.community.JpaPostStore
import com.alphatalk.coreapi.community.PostListQuery
import com.alphatalk.coreapi.community.PostRowWithAuthor
import com.alphatalk.coreapi.community.PostStore
import com.alphatalk.coreapi.stockinfo.CandleStore
import com.alphatalk.coreapi.stockinfo.DailyCandle
import com.alphatalk.coreapi.stockinfo.JpaCandleStore
import com.alphatalk.coreapi.stockinfo.StockInfoService
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.context.annotation.Primary
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Instant
import kotlin.test.assertEquals

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
@Import(ReadOnlyTransactionBoundaryTest.Probes::class)
class ReadOnlyTransactionBoundaryTest {
    companion object {
        private const val POST_ID = "01JB00000000000000000000TX"
        private const val CODE = "005930"

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

    class TransactionState {
        var active: Boolean? = null
        var readOnly: Boolean? = null

        fun capture() {
            active = TransactionSynchronizationManager.isActualTransactionActive()
            readOnly = TransactionSynchronizationManager.isCurrentTransactionReadOnly()
        }
    }

    class ProbingPostStore(private val delegate: PostStore) : PostStore by delegate {
        val onRead = TransactionState()
        val onList = TransactionState()
        val onWrite = TransactionState()

        override fun findWithAuthor(id: String): PostRowWithAuthor? {
            onRead.capture()
            return delegate.findWithAuthor(id)
        }

        override fun list(query: PostListQuery): List<PostRowWithAuthor> {
            onList.capture()
            return delegate.list(query)
        }

        override fun create(
            id: String,
            code: String,
            authorId: Long,
            title: String,
            content: String,
            quotedEventId: String?,
        ) {
            onWrite.capture()
            delegate.create(id, code, authorId, title, content, quotedEventId)
        }
    }

    class ProbingCandleStore(private val delegate: CandleStore) : CandleStore by delegate {
        val onRead = TransactionState()

        override fun findLatestUpTo(code: String, toDate: String?, limit: Int): List<DailyCandle> {
            onRead.capture()
            return delegate.findLatestUpTo(code, toDate, limit)
        }
    }

    @TestConfiguration
    class Probes {
        @Bean
        @Primary
        fun probingPostStore(delegate: JpaPostStore) = ProbingPostStore(delegate)

        @Bean
        @Primary
        fun probingCandleStore(delegate: JpaCandleStore) = ProbingCandleStore(delegate)
    }

    @Autowired
    private lateinit var community: CommunityService

    @Autowired
    private lateinit var stockInfo: StockInfoService

    @Autowired
    private lateinit var posts: ProbingPostStore

    @Autowired
    private lateinit var candles: ProbingCandleStore

    @Autowired
    private lateinit var jdbc: JdbcTemplate

    private var userId = 0L

    @BeforeEach
    fun seed() {
        jdbc.update("DELETE FROM post")
        jdbc.update("DELETE FROM users")
        jdbc.update("DELETE FROM stock_master")
        jdbc.update(
            "INSERT INTO stock_master (code, name, market, shares_outstanding, is_active) " +
                "VALUES (?, '삼성전자', 'KOSPI', 5846278000, true)",
            CODE,
        )
        jdbc.update("INSERT INTO users (email, password_hash, nickname) VALUES ('tx@b.c', 'hash', '경계검증')")
        userId = jdbc.queryForObject("SELECT id FROM users WHERE email = 'tx@b.c'", Long::class.java)!!
        jdbc.update(
            "INSERT INTO post (id, code, author_id, title, content, created_at) VALUES (?, ?, ?, '제목', '본문', ?)",
            POST_ID,
            CODE,
            userId,
            java.sql.Timestamp.from(Instant.parse("2026-08-01T00:00:00Z")),
        )
    }

    @Test
    fun `글 상세 조회는 하나의 읽기 전용 트랜잭션 안에서 실행된다`() {
        community.postDetail(userId, POST_ID)

        assertEquals(true, posts.onRead.active, "조회에 트랜잭션 경계가 없다")
        assertEquals(true, posts.onRead.readOnly, "조회 트랜잭션이 읽기 전용이 아니다")
    }

    @Test
    fun `방 글 목록 조회도 읽기 전용 트랜잭션 안에서 실행된다`() {
        community.roomPosts(CODE, null, null, null)

        assertEquals(true, posts.onList.active)
        assertEquals(true, posts.onList.readOnly)
    }

    @Test
    fun `종목 봉 조회는 읽기 전용 트랜잭션 안에서 실행된다`() {
        stockInfo.candles(CODE, "D", null, null)

        assertEquals(true, candles.onRead.active, "조회에 트랜잭션 경계가 없다")
        assertEquals(true, candles.onRead.readOnly, "조회 트랜잭션이 읽기 전용이 아니다")
    }

    @Test
    fun `글 작성은 읽기 전용이 아닌 트랜잭션에서 실행된다`() {
        community.createPost(userId, CODE, CreatePostRequest("제목", "본문"), null)

        assertEquals(true, posts.onWrite.active)
        assertEquals(false, posts.onWrite.readOnly, "쓰기 경로가 읽기 전용 트랜잭션에 묶였다")
    }
}
