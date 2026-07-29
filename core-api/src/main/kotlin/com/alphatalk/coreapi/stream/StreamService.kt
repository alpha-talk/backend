package com.alphatalk.coreapi.stream

import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode

class StreamService(
    private val stream: StreamStore,
    private val quotes: QuoteStore,
) {
    fun read(
        code: String,
        cursor: String?,
        direction: String?,
        limit: Int?,
        types: String?,
    ): StreamPage {
        val query = StreamQuery(
            code = validCode(code),
            cursor = validCursor(cursor),
            direction = validDirection(direction),
            limit = validLimit(limit),
            types = parseTypes(types),
        )
        val items = stream.find(query)
        return StreamPage(items, pageInfo(query, items))
    }

    fun quote(code: String): QuoteResponse {
        val valid = validCode(code)
        return quotes.liveQuote(valid)
            ?: quotes.lastCandleQuote(valid)
            ?: throw ApiException(ErrorCode.NOT_FOUND, "시세를 찾을 수 없습니다", mapOf("code" to valid))
    }

    private fun pageInfo(query: StreamQuery, items: List<StreamItem>): PageInfo {
        if (items.isEmpty()) {
            return PageInfo(oldest = null, newest = null, hasMoreBefore = false, hasMoreAfter = false)
        }
        val ids = items.map(StreamItem::eventId)
        val oldest = ids.min()
        val newest = ids.max()
        return PageInfo(
            oldest = oldest,
            newest = newest,
            hasMoreBefore = stream.hasOlderThan(query.code, oldest, query.types),
            hasMoreAfter = stream.hasNewerThan(query.code, newest, query.types),
        )
    }

    private fun validCode(code: String): String {
        if (!CODE_PATTERN.matches(code)) {
            throw ApiException(ErrorCode.VALIDATION_FAILED, "종목 코드는 6자리 숫자여야 합니다", mapOf("field" to "code"))
        }
        return code
    }

    private fun validCursor(cursor: String?): String? {
        val trimmed = cursor?.trim().orEmpty()
        if (trimmed.isEmpty()) return null
        if (!ULID_PATTERN.matches(trimmed)) {
            throw ApiException(ErrorCode.VALIDATION_FAILED, "cursor 형식이 올바르지 않습니다", mapOf("field" to "cursor"))
        }
        return trimmed
    }

    private fun validDirection(direction: String?): CursorDirection {
        val trimmed = direction?.trim().orEmpty()
        if (trimmed.isEmpty()) return CursorDirection.BEFORE
        return CursorDirection.fromToken(trimmed)
            ?: throw ApiException(
                ErrorCode.VALIDATION_FAILED,
                "direction은 before 또는 after여야 합니다",
                mapOf("field" to "direction"),
            )
    }

    private fun validLimit(limit: Int?): Int {
        val value = limit ?: DEFAULT_LIMIT
        if (value < MIN_LIMIT || value > MAX_LIMIT) {
            throw ApiException(
                ErrorCode.VALIDATION_FAILED,
                "limit은 $MIN_LIMIT~$MAX_LIMIT 사이여야 합니다",
                mapOf("field" to "limit"),
            )
        }
        return value
    }

    private fun parseTypes(types: String?): List<StreamEventType> {
        val raw = types?.trim().orEmpty()
        if (raw.isEmpty()) return emptyList()
        return raw.split(",").filter { it.isNotBlank() }.map { token ->
            StreamEventType.fromToken(token)
                ?: throw ApiException(
                    ErrorCode.VALIDATION_FAILED,
                    "지원하지 않는 type입니다: ${token.trim()}",
                    mapOf("field" to "types"),
                )
        }.distinct()
    }

    companion object {
        const val DEFAULT_LIMIT = 50
        const val MIN_LIMIT = 1
        const val MAX_LIMIT = 100
        private val CODE_PATTERN = Regex("^\\d{6}$")
        private val ULID_PATTERN = Regex("^[0-9A-HJKMNP-TV-Z]{26}$")
    }
}
