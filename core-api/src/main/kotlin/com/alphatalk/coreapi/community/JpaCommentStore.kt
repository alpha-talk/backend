package com.alphatalk.coreapi.community

import com.alphatalk.coreapi.auth.UserStore
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Sort
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.Clock
import java.time.Instant

@Entity
@Table(name = "comment")
class CommentEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "id", length = 26, columnDefinition = "char(26)")
    var id: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "post_id", nullable = false, length = 26, columnDefinition = "char(26)")
    var postId: String = "",
    @Column(name = "author_id", nullable = false)
    var authorId: Long = 0,
    @Column(name = "content", nullable = false)
    var content: String = "",
    @Column(name = "created_at", nullable = false)
    var createdAt: Instant = Instant.EPOCH,
    @Column(name = "deleted_at")
    var deletedAt: Instant? = null,
)

interface CommentJpaRepository : JpaRepository<CommentEntity, String> {
    @Query(
        """
        select c from CommentEntity c
        where c.postId = :postId and c.deletedAt is null
          and (:cursor is null or (:ascending = true and c.id > :cursor) or (:ascending = false and c.id < :cursor))
        """,
    )
    fun list(
        @Param("postId") postId: String,
        @Param("cursor") cursor: String?,
        @Param("ascending") ascending: Boolean,
        pageable: PageRequest,
    ): List<CommentEntity>

    fun existsByPostIdAndDeletedAtIsNullAndIdLessThan(postId: String, id: String): Boolean

    fun existsByPostIdAndDeletedAtIsNullAndIdGreaterThan(postId: String, id: String): Boolean

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update CommentEntity c set c.deletedAt = :at where c.id = :id and c.deletedAt is null")
    fun softDeleteIfActive(@Param("id") id: String, @Param("at") at: Instant): Int
}

@Repository
class JpaCommentStore(
    private val comments: CommentJpaRepository,
    private val users: UserStore,
    private val clock: Clock = Clock.systemUTC(),
) : CommentStore {
    override fun create(id: String, postId: String, authorId: Long, content: String) {
        comments.save(
            CommentEntity(
                id = id,
                postId = postId,
                authorId = authorId,
                content = content,
                createdAt = clock.instant(),
            ),
        )
    }

    override fun find(id: String): CommentRecord? =
        comments.findById(id).orElse(null)?.toRecord()

    override fun list(query: CommentListQuery): List<CommentRowWithAuthor> {
        val order = if (query.ascending) Sort.Direction.ASC else Sort.Direction.DESC
        val page = PageRequest.of(0, query.limit, Sort.by(order, "id"))
        val rows = comments.list(query.postId, query.cursor, query.ascending, page).map { it.toRecord() }
        val nicknames = users.nicknames(rows.map(CommentRecord::authorId).distinct())
        return rows.map { CommentRowWithAuthor(it, nicknames.getValue(it.authorId)) }
    }

    override fun hasOlderThan(postId: String, commentId: String): Boolean =
        comments.existsByPostIdAndDeletedAtIsNullAndIdLessThan(postId, commentId)

    override fun hasNewerThan(postId: String, commentId: String): Boolean =
        comments.existsByPostIdAndDeletedAtIsNullAndIdGreaterThan(postId, commentId)

    override fun softDeleteIfActive(id: String, at: Instant): Boolean =
        comments.softDeleteIfActive(id, at) > 0

    private fun CommentEntity.toRecord() = CommentRecord(
        id = id.trim(),
        postId = postId.trim(),
        authorId = authorId,
        content = content,
        createdAt = createdAt,
        deletedAt = deletedAt,
    )
}
