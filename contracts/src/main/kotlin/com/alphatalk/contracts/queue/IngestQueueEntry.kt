package com.alphatalk.contracts.queue

import com.alphatalk.contracts.envelope.StreamCategory

enum class IngestType(val value: String) {
    NEWS("news"),
    REPORT("report"),
    DISCLOSURE("disclosure"),
    DIGEST("digest"),
    ;

    fun streamCategory(): StreamCategory = when (this) {
        NEWS -> StreamCategory.NEWS
        REPORT -> StreamCategory.REPORT
        DISCLOSURE -> StreamCategory.DISCLOSURE
        DIGEST -> StreamCategory.AI
    }

    companion object {
        fun from(value: String): IngestType =
            entries.firstOrNull { it.value == value }
                ?: throw IllegalArgumentException("unknown ingest type: $value")
    }
}

data class IngestQueueEntry(
    val source: String,
    val sourceId: String,
    val type: IngestType,
    val codes: List<String>,
    val title: String,
    val url: String,
    val body: String? = null,
    val fetchedAt: Long,
    val macroHint: String? = null,
) {
    init {
        require(type != IngestType.DIGEST || codes.size == 1) {
            "digest entries must carry exactly one stock code"
        }
    }

    fun toFields(): Map<String, String> = buildMap {
        put(FIELD_SOURCE, source)
        put(FIELD_SOURCE_ID, sourceId)
        put(FIELD_TYPE, type.value)
        put(FIELD_CODES, codes.joinToString(","))
        put(FIELD_TITLE, title)
        put(FIELD_URL, url)
        body?.let { put(FIELD_BODY, it) }
        put(FIELD_FETCHED_AT, fetchedAt.toString())
        macroHint?.let { put(FIELD_MACRO_HINT, it) }
    }

    companion object {
        const val FIELD_SOURCE = "source"
        const val FIELD_SOURCE_ID = "sourceId"
        const val FIELD_TYPE = "type"
        const val FIELD_CODES = "codes"
        const val FIELD_TITLE = "title"
        const val FIELD_URL = "url"
        const val FIELD_BODY = "body"
        const val FIELD_FETCHED_AT = "fetchedAt"
        const val FIELD_MACRO_HINT = "macroHint"

        const val DIGEST_SOURCE = "scheduler"
        const val MARKET_CODE = "MARKET"

        fun digestSourceId(code: String, date: String) = "digest:$code:$date"

        fun fromFields(fields: Map<String, String>): IngestQueueEntry = IngestQueueEntry(
            source = fields.getValue(FIELD_SOURCE),
            sourceId = fields.getValue(FIELD_SOURCE_ID),
            type = IngestType.from(fields.getValue(FIELD_TYPE)),
            codes = fields.getValue(FIELD_CODES).split(",").filter { it.isNotBlank() },
            title = fields.getValue(FIELD_TITLE),
            url = fields.getValue(FIELD_URL),
            body = fields[FIELD_BODY],
            fetchedAt = fields.getValue(FIELD_FETCHED_AT).toLong(),
            macroHint = fields[FIELD_MACRO_HINT],
        )
    }
}
