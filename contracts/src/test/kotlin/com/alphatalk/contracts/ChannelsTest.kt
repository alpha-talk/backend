package com.alphatalk.contracts

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class ChannelsTest {
    @Test
    fun `채널명 생성`() {
        assertEquals("quote:005930", Channels.quote("005930"))
        assertEquals("stream:005930", Channels.stream("005930"))
        assertEquals("post:005930", Channels.post("005930"))
        assertEquals("trade:005930", Channels.trade("005930"))
        assertEquals("depth:005930", Channels.depth("005930"))
        assertEquals("watchlist:updated", Channels.WATCHLIST_UPDATED)
    }

    @Test
    fun `생성-파싱 라운드트립`() {
        for (kind in ChannelKind.entries.filter { it != ChannelKind.WATCHLIST }) {
            val channel = Channels.of(kind, "005930")
            val parsed = Channels.parse(channel)
            assertEquals(Channels.Parsed(kind, "005930"), parsed)
        }
    }

    @Test
    fun `watchlist 전역 채널 파싱`() {
        assertEquals(
            Channels.Parsed(ChannelKind.WATCHLIST, null),
            Channels.parse("watchlist:updated"),
        )
    }

    @Test
    fun `알 수 없는 채널은 null`() {
        assertNull(Channels.parse("unknown:005930"))
        assertNull(Channels.parse("quote"))
        assertNull(Channels.parse("quote:"))
        assertNull(Channels.parse(":005930"))
        assertNull(Channels.parse("watchlist:zzz"))
    }
}
