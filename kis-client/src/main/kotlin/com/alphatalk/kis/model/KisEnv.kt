package com.alphatalk.kis.model

enum class KisEnv(
    val restBaseUrl: String,
    val wsUrl: String,
    val restCallsPerSecond: Double,
) {
    PROD("https://openapi.koreainvestment.com:9443", "ws://ops.koreainvestment.com:21000", 20.0),
    VTS("https://openapivts.koreainvestment.com:29443", "ws://ops.koreainvestment.com:31000", 2.0),
}
