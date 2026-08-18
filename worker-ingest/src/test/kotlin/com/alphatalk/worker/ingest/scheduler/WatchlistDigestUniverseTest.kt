package com.alphatalk.worker.ingest.scheduler

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Import
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertEquals

@Entity
@Table(name = "users")
class DigestUniverseTestUser(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    val id: Long = 0,
    @Column(nullable = false)
    val email: String = "",
    @Column(name = "password_hash", nullable = false)
    val passwordHash: String = "",
    @Column(nullable = false)
    val nickname: String = "",
)

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(WatchlistDigestUniverse::class)
@Testcontainers(disabledWithoutDocker = true)
class WatchlistDigestUniverseTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )
    }

    @Autowired
    private lateinit var universe: WatchlistDigestUniverse

    @Autowired
    private lateinit var entityManager: TestEntityManager

    private fun user(email: String, nickname: String): Long =
        entityManager.persistAndGetId(
            DigestUniverseTestUser(email = email, passwordHash = "h", nickname = nickname),
            Long::class.javaObjectType,
        )

    @Test
    fun `여러 사용자가 겹치게 구독해도 종목 코드를 중복 없이 정렬해 돌려준다`() {
        val minji = user("minji@alphatalk.dev", "minji")
        val junho = user("junho@alphatalk.dev", "junho")
        entityManager.persist(WatchlistCodeEntity(userId = minji, code = "005930"))
        entityManager.persist(WatchlistCodeEntity(userId = minji, code = "000660"))
        entityManager.persist(WatchlistCodeEntity(userId = junho, code = "005930"))
        entityManager.persist(WatchlistCodeEntity(userId = junho, code = "035420"))
        entityManager.flush()
        entityManager.clear()

        assertEquals(listOf("000660", "005930", "035420"), universe.codes())
    }

    @Test
    fun `구독이 하나도 없으면 빈 목록을 돌려준다`() {
        assertEquals(emptyList(), universe.codes())
    }
}
