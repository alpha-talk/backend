package com.alphatalk.coreapi.community

enum class ReportReason {
    SPAM,
    ABUSE,
    MANIPULATION,
    ETC,
    ;

    companion object {
        fun fromToken(token: String): ReportReason? =
            entries.firstOrNull { it.name == token.trim().uppercase() }
    }
}

enum class ReportTargetType {
    POST,
}

interface ReportStore {
    fun create(
        id: String,
        targetType: ReportTargetType,
        targetId: String,
        reporterId: Long,
        reason: ReportReason,
        detail: String?,
    )
}
