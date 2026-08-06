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
        withFreshDatabase("full_apply") { connection ->
            update(connection)

            assertEquals(24, appliedChangeSetCount(connection))
            assertTrue(tableExists(connection, "news_cluster"))
            assertTrue(tableExists(connection, "stream_event"))
            assertTrue(tableExists(connection, "market_digest"))
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
            assertTrue(tableExists(connection, "dart_corp_map"))
            assertTrue(columnExists(connection, "sector", "parent_code"))
            assertTrue(columnExists(connection, "stock_master", "dart_induty_code"))
        }
    }

    @Test
    fun `KIS 업종이 들어 있는 DB를 올리면 기존 행만 KIS_MASTER로 남는다`() {
        withFreshDatabase("kis_upgrade") { connection ->
            update(connection, changeSetsThrough(connection, LAST_KIS_SECTOR_CHANGESET))
            connection.createStatement().use {
                it.execute("INSERT INTO sector (code, name) VALUES ('00027', '제조'), ('11009', '제조')")
                it.execute("INSERT INTO sector (code, name, level) VALUES ('261', '반도체 제조업', 3)")
                it.execute("UPDATE sector SET version = 'KSIC_10' WHERE code = '261'")
            }

            update(connection)

            assertEquals(mapOf("KIS_MASTER" to 2, "KSIC_10" to 1), versionCounts(connection))
        }
    }

    @Test
    fun `재적용은 멱등하고 체크섬 검증을 통과한다`() {
        withFreshDatabase("idempotent") { connection ->
            update(connection)
            val afterFirst = appliedChangeSetCount(connection)

            update(connection)

            assertEquals(afterFirst, appliedChangeSetCount(connection))
            assertEquals(0, unrunChangeSetCount(connection))
        }
    }

    private fun withFreshDatabase(name: String, block: (Connection) -> Unit) {
        adminConnection().use { admin ->
            admin.createStatement().use { it.execute("DROP DATABASE IF EXISTS $name") }
            admin.createStatement().use { it.execute("CREATE DATABASE $name") }
        }
        val url = postgres.jdbcUrl.substringBeforeLast('/') + "/$name"
        DriverManager.getConnection(url, postgres.username, postgres.password).use(block)
    }

    private fun adminConnection(): Connection =
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)

    @Suppress("DEPRECATION")
    private fun update(connection: Connection) {
        Liquibase(MASTER_CHANGELOG, ClassLoaderResourceAccessor(), database(connection))
            .update(Contexts(), LabelExpression())
    }

    @Suppress("DEPRECATION")
    private fun update(connection: Connection, changesToApply: Int) {
        Liquibase(MASTER_CHANGELOG, ClassLoaderResourceAccessor(), database(connection))
            .update(changesToApply, Contexts(), LabelExpression())
    }

    @Suppress("DEPRECATION")
    private fun changeSetsThrough(connection: Connection, changeSetId: String): Int {
        val unrun = Liquibase(MASTER_CHANGELOG, ClassLoaderResourceAccessor(), database(connection))
            .listUnrunChangeSets(Contexts(), LabelExpression())
        val index = unrun.indexOfFirst { it.id == changeSetId }
        check(index >= 0) { "changeset을 찾지 못했다: $changeSetId" }
        return index + 1
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

    private fun versionCounts(connection: Connection): Map<String, Int> =
        connection.createStatement().use { statement ->
            statement.executeQuery("SELECT version, count(*) FROM sector GROUP BY version").use { rows ->
                buildMap { while (rows.next()) put(rows.getString(1), rows.getInt(2)) }
            }
        }

    private fun tableExists(connection: Connection, table: String): Boolean =
        connection.metaData.getTables(null, "public", table, arrayOf("TABLE")).use { it.next() }

    private fun columnExists(connection: Connection, table: String, column: String): Boolean =
        connection.metaData.getColumns(null, "public", table, column).use { it.next() }

    companion object {
        private const val MASTER_CHANGELOG = "db/changelog/db.changelog-master.yaml"
        private const val LAST_KIS_SECTOR_CHANGESET = "0004-sector-ksic"
    }
}
