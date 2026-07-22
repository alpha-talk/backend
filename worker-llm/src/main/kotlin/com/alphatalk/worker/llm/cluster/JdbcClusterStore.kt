package com.alphatalk.worker.llm.cluster

import com.alphatalk.contracts.envelope.SourceRef
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant

class JdbcClusterStore(
    private val jdbc: NamedParameterJdbcTemplate,
) : ClusterStore {

    override fun findArticleCluster(sourceId: String): ClusterRecord? =
        jdbc.query(
            """
            SELECT c.id, c.rep_title, c.summary, c.scope, c.status, c.article_count
            FROM news_article a JOIN news_cluster c ON c.id = a.cluster_id
            WHERE a.source_id = :sourceId
            """,
            mapOf("sourceId" to sourceId),
            ::clusterRow,
        ).firstOrNull()

    override fun findClusterByTitleHash(titleHash: String, since: Instant): String? =
        jdbc.query(
            """
            SELECT a.cluster_id
            FROM news_article a JOIN news_cluster c ON c.id = a.cluster_id
            WHERE a.title_hash = :hash AND c.last_article_at >= :since
            ORDER BY c.last_article_at DESC LIMIT 1
            """,
            mapOf("hash" to titleHash, "since" to Timestamp.from(since)),
        ) { rs, _ -> rs.getString(1) }.firstOrNull()

    override fun nearestCluster(embedding: FloatArray, since: Instant, codes: List<String>): Pair<String, Double>? {
        val codeFilter = if (codes.isEmpty()) "" else
            "AND (c.status = 'NEW' OR EXISTS (SELECT 1 FROM news_cluster_stock s WHERE s.cluster_id = c.id AND s.code IN (:codes)))"
        val params = mutableMapOf<String, Any>(
            "vector" to toVectorLiteral(embedding),
            "since" to Timestamp.from(since),
        )
        if (codes.isNotEmpty()) params["codes"] = codes
        return jdbc.query(
            """
            SELECT a.cluster_id, 1 - (a.embedding <=> CAST(:vector AS vector)) AS similarity
            FROM news_article a JOIN news_cluster c ON c.id = a.cluster_id
            WHERE a.embedding IS NOT NULL AND c.last_article_at >= :since $codeFilter
            ORDER BY a.embedding <=> CAST(:vector AS vector) LIMIT 1
            """,
            params,
        ) { rs, _ -> rs.getString(1) to rs.getDouble(2) }.firstOrNull()
    }

    override fun createCluster(id: String, repTitle: String, publishedAt: Instant) {
        jdbc.update(
            """
            INSERT INTO news_cluster (id, rep_title, status, first_published_at, last_article_at, article_count)
            VALUES (:id, :repTitle, 'NEW', :publishedAt, :publishedAt, 0)
            """,
            mapOf("id" to id, "repTitle" to repTitle, "publishedAt" to Timestamp.from(publishedAt)),
        )
    }

    override fun attachArticle(article: ArticleRecord, clusterId: String, embedding: FloatArray?) {
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
    }

    override fun cluster(clusterId: String): ClusterRecord =
        jdbc.query(
            "SELECT id, rep_title, summary, scope, status, article_count FROM news_cluster WHERE id = :id",
            mapOf("id" to clusterId),
            ::clusterRow,
        ).first()

    override fun markSummarized(clusterId: String, summary: String, scope: String) {
        jdbc.update(
            "UPDATE news_cluster SET status = 'SUMMARIZED', summary = :summary, scope = :scope WHERE id = :id",
            mapOf("id" to clusterId, "summary" to summary, "scope" to scope),
        )
    }

    override fun markIrrelevant(clusterId: String) {
        jdbc.update(
            "UPDATE news_cluster SET status = 'IRRELEVANT' WHERE id = :id",
            mapOf("id" to clusterId),
        )
    }

    override fun stockLinks(clusterId: String): List<StockLink> =
        jdbc.query(
            "SELECT code, sentiment, confidence, stream_event_id FROM news_cluster_stock WHERE cluster_id = :id",
            mapOf("id" to clusterId),
        ) { rs, _ ->
            StockLink(
                code = rs.getString("code").trim(),
                sentiment = rs.getString("sentiment"),
                confidence = rs.getObject("confidence")?.let { (it as Number).toDouble() },
                streamEventId = rs.getString("stream_event_id"),
            )
        }

    override fun insertStockLinkIfAbsent(clusterId: String, code: String, sentiment: String?, confidence: Double?): Boolean =
        jdbc.update(
            """
            INSERT INTO news_cluster_stock (cluster_id, code, sentiment, confidence)
            VALUES (:clusterId, :code, :sentiment, :confidence)
            ON CONFLICT (cluster_id, code) DO NOTHING
            """,
            mapOf("clusterId" to clusterId, "code" to code, "sentiment" to sentiment, "confidence" to confidence),
        ) > 0

    override fun setStockLinkEvent(clusterId: String, code: String, streamEventId: String) {
        jdbc.update(
            "UPDATE news_cluster_stock SET stream_event_id = :eventId WHERE cluster_id = :clusterId AND code = :code",
            mapOf("clusterId" to clusterId, "code" to code, "eventId" to streamEventId),
        )
    }

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
            WHERE s.code = :code AND c.status = 'SUMMARIZED'
              AND c.last_article_at >= :from AND c.last_article_at < :to
            ORDER BY c.last_article_at DESC
            """,
            mapOf("code" to code, "from" to Timestamp.from(from), "to" to Timestamp.from(to)),
            ::digestRow,
        )

    override fun sectorClustersInWindow(sectorCode: String, from: Instant, to: Instant): List<DigestClusterRow> =
        jdbc.query(
            """
            SELECT c.id, c.rep_title, c.summary, s.sentiment, c.article_count, NULL AS stream_event_id
            FROM news_cluster c JOIN news_cluster_sector s ON s.cluster_id = c.id
            WHERE s.sector_code = :sectorCode AND c.status = 'SUMMARIZED'
              AND c.last_article_at >= :from AND c.last_article_at < :to
            ORDER BY c.last_article_at DESC
            """,
            mapOf("sectorCode" to sectorCode, "from" to Timestamp.from(from), "to" to Timestamp.from(to)),
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
