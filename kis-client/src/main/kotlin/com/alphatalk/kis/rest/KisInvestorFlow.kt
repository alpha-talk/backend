package com.alphatalk.kis.rest

data class KisInvestorFlow(
    val code: String,
    val date: String,
    val individual: Long,
    val foreign: Long,
    val institution: Long,
)
