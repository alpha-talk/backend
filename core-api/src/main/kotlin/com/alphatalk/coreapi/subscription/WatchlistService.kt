package com.alphatalk.coreapi.subscription

import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import org.springframework.stereotype.Service

@Service
class WatchlistService(
    private val store: WatchlistStore,
    private val catalog: StockCatalog,
    private val mirror: WatchlistMirror,
    private val announcer: WatchlistAnnouncer,
) {
    fun list(userId: Long): List<WatchlistItem> = store.list(userId)

    fun subscribe(userId: Long, rawCode: String): Boolean {
        val code = normalize(rawCode)
        val state = store.state(userId, code)
        if (!state.subscribed) {
            if (!catalog.exists(code)) {
                throw ApiException(ErrorCode.NOT_FOUND, "존재하지 않는 종목입니다", mapOf("code" to code))
            }
            if (state.total >= MAX_ITEMS) {
                throw ApiException(
                    ErrorCode.LIMIT_EXCEEDED,
                    "관심목록은 최대 ${MAX_ITEMS}개까지 담을 수 있습니다",
                    mapOf("limit" to MAX_ITEMS),
                )
            }
        }
        val added = !state.subscribed && store.add(userId, code)
        mirror.add(userId, code)
        if (added) {
            announcer.announce(userId, added = listOf(code), removed = emptyList())
        }
        return added
    }

    fun unsubscribe(userId: Long, rawCode: String) {
        val code = normalize(rawCode)
        val removed = store.remove(userId, code)
        mirror.remove(userId, code)
        if (removed) {
            announcer.announce(userId, added = emptyList(), removed = listOf(code))
        }
    }

    private fun normalize(rawCode: String): String {
        val code = rawCode.trim()
        if (!CODE_PATTERN.matches(code)) {
            throw ApiException(ErrorCode.VALIDATION_FAILED, "종목코드는 6자리 숫자입니다", mapOf("field" to "code"))
        }
        return code
    }

    companion object {
        const val MAX_ITEMS = 100
        private val CODE_PATTERN = Regex("^\\d{6}$")
    }
}
