package com.alphatalk.coreapi.subscription

import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import org.springframework.stereotype.Service

@Service
class WatchlistService(
    private val store: WatchlistStore,
    private val command: WatchlistCommand,
    private val mirror: WatchlistMirrorSync,
) {
    fun list(userId: Long): List<WatchlistItem> = store.list(userId)

    fun subscribe(userId: Long, rawCode: String): Boolean {
        val code = normalize(rawCode)
        val change = command.subscribe(userId, code, MAX_ITEMS)
        return when (change.outcome) {
            SubscribeOutcome.ADDED -> {
                mirror.sync(userId, MirrorChange(requireNotNull(change.state), addedCode = code))
                true
            }

            SubscribeOutcome.ALREADY_SUBSCRIBED -> {
                mirror.sync(userId, MirrorChange(requireNotNull(change.state), addedCode = code))
                false
            }

            SubscribeOutcome.UNKNOWN_STOCK ->
                throw ApiException(ErrorCode.NOT_FOUND, "존재하지 않는 종목입니다", mapOf("code" to code))

            SubscribeOutcome.LIMIT_EXCEEDED ->
                throw ApiException(
                    ErrorCode.LIMIT_EXCEEDED,
                    "관심목록은 최대 ${MAX_ITEMS}개까지 담을 수 있습니다",
                    mapOf("limit" to MAX_ITEMS),
                )

            SubscribeOutcome.OWNER_MISSING -> throw unauthorized()
        }
    }

    fun unsubscribe(userId: Long, rawCode: String) {
        val code = normalize(rawCode)
        val change = command.unsubscribe(userId, code)
        when (change.outcome) {
            UnsubscribeOutcome.REMOVED,
            UnsubscribeOutcome.ALREADY_REMOVED,
            -> mirror.sync(userId, MirrorChange(requireNotNull(change.state), removedCode = code))

            UnsubscribeOutcome.OWNER_MISSING -> throw unauthorized()
        }
    }

    private fun unauthorized() = ApiException(ErrorCode.UNAUTHORIZED, "인증이 필요합니다")

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
