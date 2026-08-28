package com.alphatalk.coreapi.stream

import com.alphatalk.coreapi.stream.QStreamEventEntity.streamEventEntity
import com.fasterxml.jackson.databind.ObjectMapper
import com.querydsl.core.types.dsl.BooleanExpression
import com.querydsl.jpa.impl.JPAQueryFactory
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Sort
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional

interface StreamEventIdView {
    val eventId: String
}

interface StreamInboxJpaRepository : JpaRepository<StreamEventEntity, String> {
    fun findByCodeAndEventIdGreaterThan(code: String, eventId: String, pageable: PageRequest): List<StreamEventIdView>

    fun findByCode(code: String, pageable: PageRequest): List<StreamEventIdView>

    @Query(
        """
        select e.code as code, max(e.eventId) as eventId
        from StreamEventEntity e
        where e.code in :codes
        group by e.code
        """,
    )
    fun latestByCodes(@Param("codes") codes: Collection<String>): List<LatestEventRow>

    interface LatestEventRow {
        val code: String
        val eventId: String
    }
}

@Repository
@Transactional(readOnly = true)
class JpaStreamInbox(
    private val events: StreamInboxJpaRepository,
    private val queryFactory: JPAQueryFactory,
    private val mapper: ObjectMapper,
) : StreamInbox {
    override fun countUnread(windows: List<UnreadWindow>, perCodeFetchLimit: Int): Map<String, Int> {
        if (windows.isEmpty()) return emptyMap()
        val page = PageRequest.of(0, perCodeFetchLimit, Sort.by(Sort.Direction.DESC, "eventId"))
        return windows.associate { window ->
            val unread = window.afterEventId
                ?.let { events.findByCodeAndEventIdGreaterThan(window.code, it, page) }
                ?: events.findByCode(window.code, page)
            window.code to unread.size
        }
    }

    override fun findUnread(
        windows: List<UnreadWindow>,
        types: List<StreamEventType>,
        beforeEventId: String?,
        limit: Int,
    ): List<StreamItem> {
        if (windows.isEmpty()) return emptyList()
        return queryFactory.selectFrom(streamEventEntity)
            .where(unreadIn(windows), ofTypes(types), beforeEventId?.let(::olderThan))
            .orderBy(streamEventEntity.eventId.desc())
            .limit(limit.toLong())
            .fetch()
            .map(::toItem)
    }

    override fun hasUnreadOlderThan(
        windows: List<UnreadWindow>,
        types: List<StreamEventType>,
        eventId: String,
    ): Boolean {
        if (windows.isEmpty()) return false
        return exists(unreadIn(windows), ofTypes(types), olderThan(eventId))
    }

    override fun hasUnreadNewerThan(
        windows: List<UnreadWindow>,
        types: List<StreamEventType>,
        eventId: String,
    ): Boolean {
        if (windows.isEmpty()) return false
        return exists(unreadIn(windows), ofTypes(types), newerThan(eventId))
    }

    override fun latestEventIds(codes: Collection<String>): Map<String, String> {
        if (codes.isEmpty()) return emptyMap()
        return events.latestByCodes(codes).associate { it.code.trim() to it.eventId }
    }

    private fun exists(vararg conditions: BooleanExpression?): Boolean =
        queryFactory.selectOne()
            .from(streamEventEntity)
            .where(*conditions)
            .fetchFirst() != null

    private fun unreadIn(windows: List<UnreadWindow>): BooleanExpression =
        windows.map { window ->
            val inRoom = streamEventEntity.code.eq(window.code)
            window.afterEventId
                ?.let { inRoom.and(streamEventEntity.eventId.gt(it)) }
                ?: inRoom
        }.reduce(BooleanExpression::or)

    private fun ofTypes(types: List<StreamEventType>): BooleanExpression? =
        if (types.isEmpty()) null else streamEventEntity.type.`in`(types.map(StreamEventType::storedType))

    private fun olderThan(eventId: String): BooleanExpression = streamEventEntity.eventId.lt(eventId)

    private fun newerThan(eventId: String): BooleanExpression = streamEventEntity.eventId.gt(eventId)

    private fun toItem(entity: StreamEventEntity) = StreamItem(
        eventId = entity.eventId.trim(),
        code = entity.code.trim(),
        type = entity.type,
        occurredAt = entity.occurredAt.toEpochMilli(),
        source = entity.source,
        payload = mapper.readTree(entity.payload),
    )
}
