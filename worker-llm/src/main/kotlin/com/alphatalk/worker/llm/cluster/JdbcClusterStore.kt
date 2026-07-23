package com.alphatalk.worker.llm.cluster

import com.alphatalk.contracts.envelope.SourceRef
import com.alphatalk.contracts.envelope.StreamCategory
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.annotation.Transactional
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant

@Transactional
class JdbcClusterStore(
    private val jdbc: NamedParameterJdbcTemplate,
) : ClusterStore {

    override fun findArticleCluster(sourceId: String): ClusterRecord? =
        jdbc.query(
            """
            SELECT c.id, c.rep_title, c.summary, c.scope, c.status, c.article_count, c.category
            FROM news_article a JOIN news_cluster c ON c.id = a.cluster_id
            WHERE a.source_id = :sourceId
            """,
            mapOf("sourceId" to sourceId),
            ::clusterRow,
        ).firstOrNull()

    override fun findClusterByTitleHash(titleHash: String, since: Instant, codes: List<String>): String? {
        val params = mutableMapOf<String, Any>("hash" to titleHash, "since" to Timestamp.from(since))
        val codeFilter = if (codes.isEmpty()) {
            """
            AND (
                c.scope IN ('SECTOR', 'MARKET') OR
                (c.status IN ('NEW', 'SUMMARIZING') AND NOT EXISTS (
                    SELECT 1 FROM news_cluster_stock s WHERE s.cluster_id = a.cluster_id
                ))
            )
            """.trimIndent()
        } else {
            params["codes"] = codes
            "AND EXISTS (SELECT 1 FROM news_cluster_stock s WHERE s.cluster_id = a.cluster_id AND s.code IN (:codes))"
        }
        return jdbc.query(
            """
            SELECT a.cluster_id
            FROM news_article a JOIN news_cluster c ON c.id = a.cluster_id
            WHERE a.title_hash = :hash AND c.last_article_at >= :since
              AND c.status <> 'IRRELEVANT' $codeFilter
            ORDER BY c.last_article_at DESC LIMIT 1
            """,
            params,
        ) { rs, _ -> rs.getString(1) }.firstOrNull()
    }

    override fun nearestCluster(embedding: FloatArray, since: Instant, codes: List<String>): Pair<String, Double>? {
        val params = mutableMapOf<String, Any>(
            "vector" to toVectorLiteral(embedding),
            "since" to Timestamp.from(since),
        )
        val codeFilter = if (codes.isEmpty()) {
            """
            AND (
                c.scope IN ('SECTOR', 'MARKET') OR
                (c.status IN ('NEW', 'SUMMARIZING') AND NOT EXISTS (
                    SELECT 1 FROM news_cluster_stock s WHERE s.cluster_id = c.id
                ))
            )
            """.trimIndent()
        } else {
            params["codes"] = codes
            "AND EXISTS (SELECT 1 FROM news_cluster_stock s WHERE s.cluster_id = c.id AND s.code IN (:codes))"
        }
        return jdbc.query(
            """
            SELECT a.cluster_id, 1 - (a.embedding <=> CAST(:vector AS vector)) AS similarity
            FROM news_article a JOIN news_cluster c ON c.id = a.cluster_id
            WHERE a.embedding IS NOT NULL AND c.last_article_at >= :since
              AND c.status <> 'IRRELEVANT' $codeFilter
            ORDER BY a.embedding <=> CAST(:vector AS vector) LIMIT 1
            """,
            params,
        ) { rs, _ -> rs.getString(1) to rs.getDouble(2) }.firstOrNull()
    }

    override fun createCluster(id: String, repTitle: String, publishedAt: Instant, category: StreamCategory) {
        jdbc.update(
            """
            INSERT INTO news_cluster (id, rep_title, status, category, first_published_at, last_article_at, article_count)
            VALUES (:id, :repTitle, 'NEW', :category, :publishedAt, :publishedAt, 0)
            """,
            mapOf(
                "id" to id,
                "repTitle" to repTitle,
                "category" to category.payload,
                "publishedAt" to Timestamp.from(publishedAt),
            ),
        )
    }

    override fun discardEmptyCluster(clusterId: String): Boolean =
        jdbc.update(
            """
            DELETE FROM news_cluster c
            WHERE c.id = :id AND c.status = 'NEW' AND c.article_count = 0
              AND NOT EXISTS (SELECT 1 FROM news_article a WHERE a.cluster_id = c.id)
              AND NOT EXISTS (SELECT 1 FROM news_cluster_stock s WHERE s.cluster_id = c.id)
              AND NOT EXISTS (SELECT 1 FROM news_cluster_sector s WHERE s.cluster_id = c.id)
            """,
            mapOf("id" to clusterId),
        ) > 0

    override fun attachArticle(article: ArticleRecord, clusterId: String, embedding: FloatArray?): Boolean {
        val inserted = jdbc.update(
            """
            INSERT INTO news_article
                (source, source_id, url, title, excerpt, title_hash, embedding, published_at, fetched_at, cluster_id)
            VALUES
                (:source, :sourceId, :url, :title, :excerpt, :titleHash, CAST(:embedding AS vector), :publishedAt, :fetchedAt, :clusterId)
            ON CONFLICT (source_id) DO NOTHING
            """,
            mapOf(
                "source" to article.source,
                "sourceId" to article.sourceId,
                "url" to article.url,
                "title" to article.title,
                "excerpt" to article.excerpt,
                "titleHash" to article.titleHash,
                "embedding" to embedding?.let(::toVectorLiteral),
                "publishedAt" to Timestamp.from(article.publishedAt),
                "fetchedAt" to Timestamp.from(article.fetchedAt),
                "clusterId" to clusterId,
            ),
        )
        if (inserted > 0) {
            jdbc.update(
                """
                UPDATE news_cluster
                SET article_count = article_count + 1,
                    last_article_at = GREATEST(last_article_at, :at)
                WHERE id = :id
                """,
                mapOf("id" to clusterId, "at" to Timestamp.from(article.publishedAt)),
            )
        }
        return inserted > 0
    }

    override fun addCandidateCode(clusterId: String, code: String) {
        jdbc.update(
            """
            INSERT INTO news_cluster_stock (cluster_id, code)
            VALUES (:clusterId, :code)
            ON CONFLICT (cluster_id, code) DO NOTHING
            """,
            mapOf("clusterId" to clusterId, "code" to code),
        )
    }

    override fun cluster(clusterId: String): ClusterRecord =
        jdbc.query(
            "SELECT id, rep_title, summary, scope, status, article_count, category FROM news_cluster WHERE id = :id",
            mapOf("id" to clusterId),
            ::clusterRow,
        ).first()

    override fun claimSummarize(clusterId: String, token: String, claimedAt: Instant, staleBefore: Instant): Boolean =
        jdbc.update(
            """
            UPDATE news_cluster
            SET status = 'SUMMARIZING', summarizing_at = :claimedAt, summarizing_token = :token
            WHERE id = :id AND (
                status = 'NEW' OR (
                    status = 'SUMMARIZING' AND (summarizing_at IS NULL OR summarizing_at < :staleBefore)
                )
            )
            """,
            mapOf(
                "id" to clusterId,
                "token" to token,
                "claimedAt" to Timestamp.from(claimedAt),
                "staleBefore" to Timestamp.from(staleBefore),
            ),
        ) > 0

    override fun renewSummarize(clusterId: String, token: String, renewedAt: Instant): Boolean =
        jdbc.update(
            """
            UPDATE news_cluster SET summarizing_at = :renewedAt
            WHERE id = :id AND status = 'SUMMARIZING' AND summarizing_token = :token
            """,
            mapOf("id" to clusterId, "token" to token, "renewedAt" to Timestamp.from(renewedAt)),
        ) > 0

    override fun markSummarized(clusterId: String, token: String, summary: String, scope: String): Boolean =
        jdbc.update(
            """
            UPDATE news_cluster
            SET status = 'SUMMARIZED', summary = :summary, scope = :scope,
                summarizing_at = NULL, summarizing_token = NULL
            WHERE id = :id AND status = 'SUMMARIZING' AND summarizing_token = :token
            """,
            mapOf("id" to clusterId, "token" to token, "summary" to summary, "scope" to scope),
        ) > 0

    override fun markIrrelevant(clusterId: String, token: String): Boolean =
        jdbc.update(
            """
            UPDATE news_cluster
            SET status = 'IRRELEVANT', summarizing_at = NULL, summarizing_token = NULL
            WHERE id = :id AND status = 'SUMMARIZING' AND summarizing_token = :token
            """,
            mapOf("id" to clusterId, "token" to token),
        ) > 0

    override fun stockLinks(clusterId: String): List<StockLink> =
        jdbc.query(
            "SELECT code, sentiment, confidence, stream_event_id, rejected FROM news_cluster_stock WHERE cluster_id = :id",
            mapOf("id" to clusterId),
        ) { rs, _ ->
            StockLink(
                code = rs.getString("code").trim(),
                sentiment = rs.getString("sentiment"),
                confidence = rs.getObject("confidence")?.let { (it as Number).toDouble() },
                streamEventId = rs.getString("stream_event_id"),
                rejected = rs.getObject("rejected") as Boolean?,
            )
        }

    override fun applyStockVerdict(
        clusterId: String,
        code: String,
        sentiment: String?,
        confidence: Double?,
        rejected: Boolean,
    ) {
        jdbc.update(
            """
            INSERT INTO news_cluster_stock (cluster_id, code, sentiment, confidence, rejected)
            VALUES (:clusterId, :code, :sentiment, :confidence, :rejected)
            ON CONFLICT (cluster_id, code)
            DO UPDATE SET sentiment = :sentiment, confidence = :confidence, rejected = :rejected
            """,
            mapOf(
                "clusterId" to clusterId,
                "code" to code,
                "sentiment" to sentiment,
                "confidence" to confidence,
                "rejected" to rejected,
            ),
        )
    }

    override fun claimStockEvent(clusterId: String, code: String, eventId: String): Boolean =
        jdbc.update(
            """
            UPDATE news_cluster_stock SET stream_event_id = :eventId
            WHERE cluster_id = :clusterId AND code = :code AND stream_event_id IS NULL
            """,
            mapOf("clusterId" to clusterId, "code" to code, "eventId" to eventId),
        ) > 0

    override fun upsertSectorLink(clusterId: String, sectorCode: String, sentiment: String, confidence: Double, impact: String) {
        jdbc.update(
            """
            INSERT INTO news_cluster_sector (cluster_id, sector_code, sentiment, confidence, impact)
            VALUES (:clusterId, :sectorCode, :sentiment, :confidence, :impact)
            ON CONFLICT (cluster_id, sector_code)
            DO UPDATE SET sentiment = :sentiment, confidence = :confidence, impact = :impact
            """,
            mapOf(
                "clusterId" to clusterId,
                "sectorCode" to sectorCode,
                "sentiment" to sentiment,
                "confidence" to confidence,
                "impact" to impact,
            ),
        )
    }

    override fun articleSources(clusterId: String): List<SourceRef> =
        jdbc.query(
            "SELECT source, url FROM news_article WHERE cluster_id = :id ORDER BY id",
            mapOf("id" to clusterId),
        ) { rs, _ -> SourceRef(name = rs.getString("source"), url = rs.getString("url")) }

    override fun representativeUrl(clusterId: String): String? =
        jdbc.query(
            "SELECT url FROM news_article WHERE cluster_id = :id ORDER BY id LIMIT 1",
            mapOf("id" to clusterId),
        ) { rs, _ -> rs.getString(1) }.firstOrNull()

    override fun stockClustersInWindow(code: String, from: Instant, to: Instant): List<DigestClusterRow> =
        jdbc.query(
            """
            SELECT c.id, c.rep_title, c.summary, s.sentiment, c.article_count, s.stream_event_id
            FROM news_cluster c JOIN news_cluster_stock s ON s.cluster_id = c.id
            WHERE s.code = :code AND s.stream_event_id IS NOT NULL
              AND c.status = 'SUMMARIZED' AND c.scope = 'STOCK'
              AND c.last_article_at >= :from AND c.last_article_at < :to
            ORDER BY c.last_article_at DESC
            """,
            mapOf("code" to code, "from" to Timestamp.from(from), "to" to Timestamp.from(to)),
            ::digestRow,
        )

    override fun sectorClustersInWindow(
        sectorCode: String,
        stockCode: String,
        from: Instant,
        to: Instant,
    ): List<DigestClusterRow> =
        jdbc.query(
            """
            SELECT c.id, c.rep_title, c.summary, s.sentiment, c.article_count, stock.stream_event_id
            FROM news_cluster c JOIN news_cluster_sector s ON s.cluster_id = c.id
            LEFT JOIN news_cluster_stock stock ON stock.cluster_id = c.id AND stock.code = :stockCode
            WHERE s.sector_code = :sectorCode AND c.status = 'SUMMARIZED' AND c.scope = 'SECTOR'
              AND c.last_article_at >= :from AND c.last_article_at < :to
            ORDER BY c.last_article_at DESC
            """,
            mapOf(
                "sectorCode" to sectorCode,
                "stockCode" to stockCode,
                "from" to Timestamp.from(from),
                "to" to Timestamp.from(to),
            ),
            ::digestRow,
        )

    override fun marketClustersInWindow(from: Instant, to: Instant): List<DigestClusterRow> =
        jdbc.query(
            """
            SELECT c.id, c.rep_title, c.summary, NULL AS sentiment, c.article_count, NULL AS stream_event_id
            FROM news_cluster c
            WHERE c.scope = 'MARKET' AND c.status = 'SUMMARIZED'
              AND c.last_article_at >= :from AND c.last_article_at < :to
            ORDER BY c.last_article_at DESC
            """,
            mapOf("from" to Timestamp.from(from), "to" to Timestamp.from(to)),
            ::digestRow,
        )

    private fun clusterRow(rs: ResultSet, @Suppress("UNUSED_PARAMETER") rowNum: Int) = ClusterRecord(
        id = rs.getString("id"),
        repTitle = rs.getString("rep_title"),
        summary = rs.getString("summary"),
        scope = rs.getString("scope"),
        status = ClusterStatus.valueOf(rs.getString("status")),
        articleCount = rs.getInt("article_count"),
        category = rs.getString("category").let {
            StreamCategory.fromPayload(it) ?: throw IllegalStateException("unknown cluster category: $it")
        },
    )

    private fun digestRow(rs: ResultSet, @Suppress("UNUSED_PARAMETER") rowNum: Int) = DigestClusterRow(
        clusterId = rs.getString(1),
        title = rs.getString(2),
        summary = rs.getString(3) ?: "",
        sentiment = rs.getString(4),
        articleCount = rs.getInt(5),
        streamEventId = rs.getString(6),
    )

    private fun toVectorLiteral(embedding: FloatArray): String =
        embedding.joinToString(",", prefix = "[", postfix = "]")
}
