package com.alphatalk.dbmigrations

import java.sql.Connection
import java.sql.DriverManager
import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.database.Database
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MasterChangelogTest {

    private val postgres = PostgreSQLContainer(
        DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
    ).also { it.start() }

    @AfterAll
    fun tearDown() {
        postgres.stop()
    }

    @Test
    fun `빈 PostgreSQL에 마스터 changelog 전체가 적용된다`() {
        withConnection { connection ->
            update(connection)

            assertEquals(18, appliedChangeSetCount(connection))
            assertTrue(tableExists(connection, "news_cluster"))
            assertTrue(tableExists(connection, "stream_event"))
            assertTrue(tableExists(connection, "daily_candle"))
            assertTrue(tableExists(connection, "minute_candle"))
            assertTrue(tableExists(connection, "stock_master"))
            assertTrue(tableExists(connection, "sector"))
            assertTrue(tableExists(connection, "batch_job_run"))
            assertTrue(tableExists(connection, "users"))
            assertTrue(tableExists(connection, "refresh_tokens"))
            assertTrue(tableExists(connection, "watchlist"))
            assertTrue(tableExists(connection, "watchlist_rev"))
            assertTrue(tableExists(connection, "read_cursor"))
            assertTrue(tableExists(connection, "post"))
            assertTrue(tableExists(connection, "comment"))
            assertTrue(tableExists(connection, "post_like"))
            assertTrue(tableExists(connection, "report"))
            assertTrue(tableExists(connection, "idempotency_record"))
            assertTrue(tableExists(connection, "valuation_daily"))
            assertTrue(tableExists(connection, "investor_flow_daily"))
            assertTrue(tableExists(connection, "financial_summary"))
        }
    }

    @Test
    fun `재적용은 멱등하고 체크섬 검증을 통과한다`() {
        withConnection { connection ->
            update(connection)
            val afterFirst = appliedChangeSetCount(connection)

            update(connection)

            assertEquals(afterFirst, appliedChangeSetCount(connection))
            assertEquals(0, unrunChangeSetCount(connection))
        }
    }

    private fun withConnection(block: (Connection) -> Unit) {
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use(block)
    }

    @Suppress("DEPRECATION")
    private fun update(connection: Connection) {
        Liquibase(MASTER_CHANGELOG, ClassLoaderResourceAccessor(), database(connection))
            .update(Contexts(), LabelExpression())
    }

    @Suppress("DEPRECATION")
    private fun unrunChangeSetCount(connection: Connection): Int =
        Liquibase(MASTER_CHANGELOG, ClassLoaderResourceAccessor(), database(connection))
            .listUnrunChangeSets(Contexts(), LabelExpression())
            .size

    private fun database(connection: Connection): Database =
        DatabaseFactory.getInstance().findCorrectDatabaseImplementation(JdbcConnection(connection))

    private fun appliedChangeSetCount(connection: Connection): Int =
        connection.createStatement().use { statement ->
            statement.executeQuery("SELECT count(*) FROM databasechangelog").use { rows ->
                rows.next()
                rows.getInt(1)
            }
        }

    private fun tableExists(connection: Connection, table: String): Boolean =
        connection.metaData.getTables(null, "public", table, arrayOf("TABLE")).use { it.next() }

    companion object {
        private const val MASTER_CHANGELOG = "db/changelog/db.changelog-master.yaml"
    }
}
