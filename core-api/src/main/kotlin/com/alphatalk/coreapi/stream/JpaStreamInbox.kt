package com.alphatalk.coreapi.stream

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Sort
import org.springframework.data.jpa.domain.Specification
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository

interface StreamEventIdView {
    val eventId: String
}

interface StreamInboxJpaRepository :
    org.springframework.data.jpa.repository.JpaRepository<StreamEventEntity, String>,
    org.springframework.data.jpa.repository.JpaSpecificationExecutor<StreamEventEntity> {
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
class JpaStreamInbox(
    private val events: StreamInboxJpaRepository,
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
        val spec = unreadIn(windows)
            .and(ofTypes(types))
            .and(beforeEventId?.let(::olderThan))
        return events.findBy<StreamEventEntity, List<StreamEventEntity>>(spec) {
            it.sortBy(Sort.by(Sort.Direction.DESC, "eventId"))
                .limit(limit)
                .all()
        }
            .map(::toItem)
    }

    override fun hasUnreadOlderThan(
        windows: List<UnreadWindow>,
        types: List<StreamEventType>,
        eventId: String,
    ): Boolean {
        if (windows.isEmpty()) return false
        return events.exists(unreadIn(windows).and(ofTypes(types)).and(olderThan(eventId)))
    }

    override fun hasUnreadNewerThan(
        windows: List<UnreadWindow>,
        types: List<StreamEventType>,
        eventId: String,
    ): Boolean {
        if (windows.isEmpty()) return false
        return events.exists(unreadIn(windows).and(ofTypes(types)).and(newerThan(eventId)))
    }

    override fun latestEventIds(codes: Collection<String>): Map<String, String> {
        if (codes.isEmpty()) return emptyMap()
        return events.latestByCodes(codes).associate { it.code.trim() to it.eventId }
    }

    private fun unreadIn(windows: List<UnreadWindow>) = Specification<StreamEventEntity> { root, _, builder ->
        builder.or(
            *windows.map { window ->
                val inRoom = builder.equal(root.get<String>("code"), window.code)
                window.afterEventId
                    ?.let { builder.and(inRoom, builder.greaterThan(root.get("eventId"), it)) }
                    ?: inRoom
            }.toTypedArray(),
        )
    }

    private fun ofTypes(types: List<StreamEventType>) = Specification<StreamEventEntity> { root, _, _ ->
        if (types.isEmpty()) null else root.get<String>("type").`in`(types.map(StreamEventType::storedType))
    }

    private fun olderThan(eventId: String) = Specification<StreamEventEntity> { root, _, builder ->
        builder.lessThan(root.get("eventId"), eventId)
    }

    private fun newerThan(eventId: String) = Specification<StreamEventEntity> { root, _, builder ->
        builder.greaterThan(root.get("eventId"), eventId)
    }

    private fun toItem(entity: StreamEventEntity) = StreamItem(
        eventId = entity.eventId.trim(),
        code = entity.code.trim(),
        type = entity.type,
        occurredAt = entity.occurredAt.toEpochMilli(),
        source = entity.source,
        payload = mapper.readTree(entity.payload),
    )
}
