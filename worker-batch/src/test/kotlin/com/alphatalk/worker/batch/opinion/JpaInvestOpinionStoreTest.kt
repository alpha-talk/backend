package com.alphatalk.worker.batch.opinion

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Import
import org.junit.jupiter.api.AfterEach
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@DataJpaTest(properties = ["spring.liquibase.change-log=classpath:db/changelog/db.changelog-master.yaml"])
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaInvestOpinionStore::class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Testcontainers(disabledWithoutDocker = true)
class JpaInvestOpinionStoreTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )
    }

    @Autowired
    private lateinit var store: JpaInvestOpinionStore

    @Autowired
    private lateinit var opinions: InvestOpinionJpaRepository

    @Autowired
    private lateinit var events: StreamEventJpaRepository

    @AfterEach
    fun cleanUp() {
        opinions.deleteAll()
        events.deleteAll()
    }

    private fun binder() = JpaOpinionEventBinder(events, opinions, jacksonObjectMapper())

    private fun observation(rating: String = "매수", targetPrice: Long? = 95000) = OpinionObservation(
        code = "005930",
        businessDate = "20260807",
        brokerCode = "00005",
        brokerName = "미래에셋",
        rating = rating,
        previousRating = "중립",
        targetPrice = targetPrice,
        contentHash = OpinionObservation.contentHash(rating, "중립", targetPrice),
        collectedAt = Instant.parse("2026-08-07T01:00:00Z"),
    )

    @Test
    fun `동일 observation 재삽입은 무시되고 published_at을 되돌리지 않는다`() {
        val o = observation()
        assertTrue(store.insertIfAbsent(o))
        assertFalse(store.insertIfAbsent(o))

        val eventId = binder().ensureEvent(UnpublishedOpinion(o, null))
        assertTrue(store.markPublished(eventId, Instant.now()))

        assertFalse(store.insertIfAbsent(o))
        val row = opinions.findById(o.key()).orElseThrow()
        assertNotNull(row.publishedAt)
    }

    @Test
    fun `중복이 아닌 무결성 위반은 삼키지 않고 전파한다`() {
        val invalid = observation().copy(code = "1234567")

        kotlin.test.assertFailsWith<org.springframework.dao.DataIntegrityViolationException> {
            store.insertIfAbsent(invalid)
        }
    }

    @Test
    fun `의견이나 목표가가 바뀌면 별도 observation이 된다`() {
        assertTrue(store.insertIfAbsent(observation()))
        assertTrue(store.insertIfAbsent(observation(targetPrice = 90000)))
        assertEquals(2, store.findUnpublished(10).size)
    }

    @Test
    fun `미통보 스캔은 발행 완료 행을 제외한다`() {
        val o = observation()
        store.insertIfAbsent(o)
        val eventId = binder().ensureEvent(UnpublishedOpinion(o, null))
        store.markPublished(eventId, Instant.now())

        assertTrue(store.findUnpublished(10).isEmpty())
    }

    @Test
    fun `ensureEvent는 source_key 충돌 시 기존 eventId를 재사용하고 observation에 연결한다`() {
        val o = observation()
        store.insertIfAbsent(o)

        val first = binder().ensureEvent(UnpublishedOpinion(o, null))
        val second = binder().ensureEvent(UnpublishedOpinion(o, null))

        assertEquals(first, second)
        assertEquals(1, events.count())
        val row = opinions.findById(o.key()).orElseThrow()
        assertEquals(first, row.streamEventId)
        val event = events.findById(first).orElseThrow()
        assertEquals(o.sourceKey, event.sourceKey)
        assertEquals("REPORT", event.type)
        assertEquals("00005", event.source)
        val payload = jacksonObjectMapper().readTree(event.payload)
        assertEquals("opinion", payload.path("kind").asText())
        assertEquals("report", payload.path("category").asText())
    }

    @Test
    fun `이미 연결된 observation은 그 eventId를 그대로 쓴다`() {
        val o = observation()
        store.insertIfAbsent(o)
        val linked = binder().ensureEvent(UnpublishedOpinion(o, null))

        val resumed = binder().ensureEvent(UnpublishedOpinion(o, linked))

        assertEquals(linked, resumed)
        assertEquals(1, events.count())
    }

    @Test
    fun `markPublished는 미발행 행만 마킹한다`() {
        val o = observation()
        store.insertIfAbsent(o)
        val eventId = binder().ensureEvent(UnpublishedOpinion(o, null))

        assertTrue(store.markPublished(eventId, Instant.now()))
        assertFalse(store.markPublished(eventId, Instant.now()))
        assertFalse(store.markPublished("01UNKNOWN000000000000000000", Instant.now()))
    }
}
