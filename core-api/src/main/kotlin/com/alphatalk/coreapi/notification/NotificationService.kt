package com.alphatalk.coreapi.notification

import com.alphatalk.coreapi.search.StockCatalog
import com.alphatalk.coreapi.stream.PageInfo
import com.alphatalk.coreapi.stream.StreamEventType
import com.alphatalk.coreapi.stream.StreamInbox
import com.alphatalk.coreapi.stream.UnreadWindow
import com.alphatalk.coreapi.subscription.WatchlistStore
import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import org.springframework.stereotype.Service

@Service
class NotificationService(
    private val watchlist: WatchlistStore,
    private val inbox: StreamInbox,
    private val cursorStore: ReadCursorStore,
    private val cursorCache: CursorCache,
    private val badgeCache: BadgeCache,
    private val stocks: StockCatalog,
    private val opinionFeed: OpinionFeed,
    private val opinionCursors: OpinionReadCursorStore,
) {
    fun badge(userId: Long): BadgeResponse {
        badgeCache.find(userId)?.let { return it }
        val windows = unreadWindows(userId)
        val byCode = inbox.countUnread(windows, BADGE_FETCH_LIMIT)
            .mapValues { (_, count) -> minOf(count, BADGE_CAP) }
            .filterValues { it > 0 }
        val opinions = minOf(opinionFeed.countNewerThan(opinionCursors.find(userId), BADGE_FETCH_LIMIT), BADGE_CAP)
        val badge = BadgeResponse(total = byCode.values.sum(), byCode = byCode, opinions = opinions)
        badgeCache.store(userId, badge)
        return badge
    }

    fun list(userId: Long, types: String?, cursor: String?, limit: Int?): NotificationPage {
        val validCursor = validCursor(cursor)
        val validLimit = validLimit(limit)
        val validTypes = parseTypes(types)
        val windows = unreadWindows(userId)
        val items = inbox.findUnread(windows, validTypes, validCursor, validLimit)
        if (items.isEmpty()) {
            return NotificationPage(items, PageInfo(oldest = null, newest = null, hasMoreBefore = false, hasMoreAfter = false))
        }
        val oldest = items.last().eventId
        val newest = items.first().eventId
        return NotificationPage(
            items,
            PageInfo(
                oldest = oldest,
                newest = newest,
                hasMoreBefore = inbox.hasUnreadOlderThan(windows, validTypes, oldest),
                hasMoreAfter = validCursor != null && inbox.hasUnreadNewerThan(windows, validTypes, newest),
            ),
        )
    }

    fun advanceCursor(userId: Long, rawCode: String, rawEventId: String?) {
        val code = validCode(rawCode)
        val eventId = validCursor(rawEventId?.trim())
            ?: throw ApiException(ErrorCode.VALIDATION_FAILED, "lastEventId는 필수입니다", mapOf("field" to "lastEventId"))
        if (!stocks.existsActive(code)) {
            throw ApiException(ErrorCode.NOT_FOUND, "존재하지 않는 종목입니다", mapOf("code" to code))
        }
        val finalCursor = cursorStore.advance(userId, code, eventId)
        cursorCache.advance(userId, code, finalCursor)
        badgeCache.evict(userId)
    }

    fun opinions(cursor: String?, limit: Int?): OpinionPage {
        val validCursor = validCursor(cursor)
        val validLimit = validLimit(limit)
        val items = opinionFeed.findLatest(validCursor, validLimit).map(::toOpinionItem)
        if (items.isEmpty()) {
            return OpinionPage(items, PageInfo(oldest = null, newest = null, hasMoreBefore = false, hasMoreAfter = false))
        }
        val oldest = items.last().eventId
        val newest = items.first().eventId
        return OpinionPage(
            items,
            PageInfo(
                oldest = oldest,
                newest = newest,
                hasMoreBefore = opinionFeed.hasOlderThan(oldest),
                hasMoreAfter = validCursor != null && opinionFeed.hasNewerThan(newest),
            ),
        )
    }

    fun advanceOpinionCursor(userId: Long, rawEventId: String?) {
        val eventId = validCursor(rawEventId?.trim())
            ?: throw ApiException(ErrorCode.VALIDATION_FAILED, "lastEventId는 필수입니다", mapOf("field" to "lastEventId"))
        opinionCursors.advance(userId, eventId)
        badgeCache.evict(userId)
    }

    fun readAll(userId: Long) {
        val codes = watchlist.codes(userId)
        if (codes.isNotEmpty()) {
            val latest = inbox.latestEventIds(codes)
            if (latest.isNotEmpty()) {
                val finalCursors = cursorStore.advanceAll(userId, latest)
                cursorCache.advanceAll(userId, finalCursors)
            }
        }
        opinionFeed.latestEventId()?.let { opinionCursors.advance(userId, it) }
        badgeCache.evict(userId)
    }

    private fun toOpinionItem(record: OpinionRecord) = OpinionItem(
        eventId = record.eventId,
        code = record.code,
        businessDate = record.businessDate,
        brokerCode = record.brokerCode,
        brokerName = record.brokerName,
        rating = record.rating,
        previousRating = record.previousRating,
        targetPrice = record.targetPrice,
        collectedAt = record.collectedAt.toEpochMilli(),
    )

    private fun unreadWindows(userId: Long): List<UnreadWindow> {
        val codes = watchlist.codes(userId)
        if (codes.isEmpty()) return emptyList()
        val cursors = resolveCursors(userId, codes)
        return codes.map { UnreadWindow(it, cursors[it]) }
    }

    private fun resolveCursors(userId: Long, codes: List<String>): Map<String, String> {
        val cached = cursorCache.read(userId, codes)
            ?: return cursorStore.find(userId, codes)
        val missing = codes.filterNot(cached::containsKey)
        if (missing.isEmpty()) return cached
        val mirrored = cursorStore.find(userId, missing)
        mirrored.forEach { (code, eventId) -> cursorCache.advance(userId, code, eventId) }
        return cached + mirrored
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
        const val DEFAULT_LIMIT = 30
        const val MIN_LIMIT = 1
        const val MAX_LIMIT = 100
        const val BADGE_CAP = 99
        const val BADGE_FETCH_LIMIT = 100
        private val CODE_PATTERN = Regex("^\\d{6}$")
        private val ULID_PATTERN = Regex("^[0-9A-HJKMNP-TV-Z]{26}$")
    }
}
