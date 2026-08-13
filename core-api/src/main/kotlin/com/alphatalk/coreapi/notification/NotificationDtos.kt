package com.alphatalk.coreapi.notification

import com.alphatalk.coreapi.stream.PageInfo
import com.alphatalk.coreapi.stream.StreamItem

data class BadgeResponse(
    val total: Int,
    val byCode: Map<String, Int>,
    val opinions: Int = 0,
)

data class NotificationPage(
    val items: List<StreamItem>,
    val pageInfo: PageInfo,
)

data class OpinionItem(
    val eventId: String,
    val code: String,
    val businessDate: String,
    val brokerCode: String,
    val brokerName: String?,
    val rating: String,
    val previousRating: String?,
    val targetPrice: Long?,
    val collectedAt: Long,
)

data class OpinionPage(
    val items: List<OpinionItem>,
    val pageInfo: PageInfo,
)

data class CursorAdvanceRequest(
    val lastEventId: String?,
)
