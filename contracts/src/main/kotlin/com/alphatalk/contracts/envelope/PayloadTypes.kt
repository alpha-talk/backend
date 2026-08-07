package com.alphatalk.contracts.envelope

import com.fasterxml.jackson.annotation.JsonInclude

data class QuoteData(
    val price: Long,
    val prevClose: Long,
    val change: Long,
    val changeRate: Double,
    val volume: Long,
    val open: Long,
    val high: Long,
    val low: Long,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class StreamData(
    val category: String,
    val title: String,
    val summary: String? = null,
    val sourceUrl: String? = null,
    val occurredAt: Long,
    val sentiment: String? = null,
    val scope: String? = null,
    val sector: SectorRef? = null,
    val sources: List<SourceRef>? = null,
    val digest: DigestData? = null,
    val kind: String? = null,
    val opinion: OpinionData? = null,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class OpinionData(
    val brokerCode: String,
    val brokerName: String? = null,
    val rating: String,
    val previousRating: String? = null,
    val targetPrice: Long? = null,
    val businessDate: String,
) {
    companion object {
        const val KIND = "opinion"
    }
}

enum class Sentiment { POSITIVE, NEGATIVE, NEUTRAL }

enum class NewsScope { STOCK, SECTOR, MARKET }

data class SectorRef(val code: String, val name: String)

data class SourceRef(val name: String, val url: String)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class DigestData(
    val date: String,
    val positives: List<Item> = emptyList(),
    val negatives: List<Item> = emptyList(),
    val sectorIssues: List<SectorIssue> = emptyList(),
    val marketIssues: List<MarketIssue> = emptyList(),
    val marketAnalysis: MarketAnalysis? = null,
    val neutralCount: Int = 0,
    val newsCount: Int = 0,
) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    data class Item(val title: String, val line: String, val eventId: String? = null)

    @JsonInclude(JsonInclude.Include.NON_NULL)
    data class SectorIssue(val title: String, val line: String, val sentiment: String, val eventId: String? = null)

    data class MarketIssue(val title: String, val line: String)
}

@JsonInclude(JsonInclude.Include.NON_NULL)
data class MarketAnalysis(
    val summary: String,
    val domestic: List<DomesticItem> = emptyList(),
    val global: List<GlobalItem> = emptyList(),
    val sources: List<ResearchSource> = emptyList(),
    val asOf: String,
    val factDate: String? = null,
    val degraded: Boolean = false,
) {
    data class DomesticItem(val title: String, val line: String)

    data class GlobalItem(val title: String, val line: String, val sourceIds: List<String>)

    @JsonInclude(JsonInclude.Include.NON_NULL)
    data class ResearchSource(val id: String, val title: String, val url: String, val publisher: String? = null)
}

data class PostData(
    val kind: String,
    val postId: String,
    val parentId: String? = null,
    val author: Author,
    val content: String,
    val createdAt: Long,
) {
    data class Author(val id: Long, val nickname: String)
}
