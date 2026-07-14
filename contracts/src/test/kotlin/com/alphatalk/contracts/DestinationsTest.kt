package com.alphatalk.contracts

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class DestinationsTest {
    @Test
    fun `방 토픽 생성-파싱 라운드트립`() {
        assertEquals(
            Destinations.RoomTopic("005930", ChannelKind.POST),
            Destinations.parseRoomTopic(Destinations.roomPosts("005930")),
        )
        assertEquals(
            Destinations.RoomTopic("005930", ChannelKind.TRADE),
            Destinations.parseRoomTopic(Destinations.roomTrade("005930")),
        )
        assertEquals(
            Destinations.RoomTopic("005930", ChannelKind.DEPTH),
            Destinations.parseRoomTopic(Destinations.roomDepth("005930")),
        )
    }

    @Test
    fun `방 토픽이 아니면 null`() {
        assertNull(Destinations.parseRoomTopic("/user/queue/quote"))
        assertNull(Destinations.parseRoomTopic("/topic/rooms/005930/other"))
        assertNull(Destinations.parseRoomTopic("/topic/rooms/12345/posts"))
        assertNull(Destinations.parseRoomTopic("/topic/rooms/abcdef/posts"))
        assertNull(Destinations.parseRoomTopic("/topic/rooms/005930/posts/x"))
    }
}
