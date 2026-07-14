package com.alphatalk.contracts.envelope

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_NULL)
data class Envelope<T>(
    val type: String,
    val code: String,
    val eventId: String? = null,
    val ts: Long,
    val data: T,
)
