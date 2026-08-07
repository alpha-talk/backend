package com.alphatalk.kis.rest

data class KisInvestOpinion(
    val code: String,
    val businessDate: String,
    val rating: String,
    val previousRating: String?,
    val targetPrice: Long?,
    val memberName: String?,
)
