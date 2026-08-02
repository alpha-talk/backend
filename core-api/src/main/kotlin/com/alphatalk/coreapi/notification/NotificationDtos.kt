package com.alphatalk.coreapi.notification

import com.alphatalk.coreapi.stream.PageInfo
import com.alphatalk.coreapi.stream.StreamItem

data class BadgeResponse(
    val total: Int,
    val byCode: Map<String, Int>,
)

data class NotificationPage(
    val items: List<StreamItem>,
    val pageInfo: PageInfo,
)

data class CursorAdvanceRequest(
    val lastEventId: String?,
)
