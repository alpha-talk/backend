package com.alphatalk.contracts

object Channels {
    const val WATCHLIST_UPDATED = "watchlist:updated"

    const val DEMAND_UPDATED = "demand:updated"

    fun quote(code: String) = "${ChannelKind.QUOTE.prefix}:$code"
    fun stream(code: String) = "${ChannelKind.STREAM.prefix}:$code"
    fun post(code: String) = "${ChannelKind.POST.prefix}:$code"
    fun trade(code: String) = "${ChannelKind.TRADE.prefix}:$code"
    fun depth(code: String) = "${ChannelKind.DEPTH.prefix}:$code"

    fun of(kind: ChannelKind, code: String): String {
        require(kind != ChannelKind.WATCHLIST) { "watchlist channel has no code" }
        return "${kind.prefix}:$code"
    }

    data class Parsed(val kind: ChannelKind, val code: String?)

    fun parse(channel: String): Parsed? {
        if (channel == WATCHLIST_UPDATED) return Parsed(ChannelKind.WATCHLIST, null)
        val sep = channel.indexOf(':')
        if (sep <= 0 || sep == channel.length - 1) return null
        val prefix = channel.substring(0, sep)
        val code = channel.substring(sep + 1)
        val kind = ChannelKind.entries.firstOrNull { it.prefix == prefix && it != ChannelKind.WATCHLIST }
            ?: return null
        return Parsed(kind, code)
    }
}
