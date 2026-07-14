package com.alphatalk.contracts

enum class ChannelKind(val prefix: String) {
    QUOTE("quote"),
    STREAM("stream"),
    POST("post"),
    TRADE("trade"),
    DEPTH("depth"),

    WATCHLIST("watchlist"),
}
