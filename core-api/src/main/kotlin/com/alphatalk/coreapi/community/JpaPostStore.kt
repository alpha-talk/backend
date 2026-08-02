package com.alphatalk.coreapi.community

import com.alphatalk.coreapi.auth.UserStore
import com.alphatalk.coreapi.stream.CursorDirection
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
@Table(name = "post")
class PostEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "id", length = 26, columnDefinition = "char(26)")
    var id: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6, columnDefinition = "char(6)")
    var code: String = "",
    @Column(name = "author_id", nullable = false)
    var authorId: Long = 0,
    @Column(name = "title", nullable = false)
    var title: String = "",
    @Column(name = "content", nullable = false)
    var content: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "quoted_event_id", length = 26, columnDefinition = "char(26)")
    var quotedEventId: String? = null,
    @Column(name = "like_count", nullable = false)
    var likeCount: Int = 0,
    @Column(name = "comment_count", nullable = false)
    var commentCount: Int = 0,
    @Column(name = "created_at", nullable = false)
    var createdAt: Instant = Instant.EPOCH,
    @Column(name = "updated_at")
    var updatedAt: Instant? = null,
    @Column(name = "deleted_at")
    var deletedAt: Instant? = null,
)

interface PostJpaRepository : JpaRepository<PostEntity, String> {
    @Query(
        """
        select p from PostEntity p
        where p.code = :code and p.deletedAt is null
          and (:cursor is null or (:ascending = true and p.id > :cursor) or (:ascending = false and p.id < :cursor))
        """,
    )
    fun listRoom(
        @Param("code") code: String,
        @Param("cursor") cursor: String?,
        @Param("ascending") ascending: Boolean,
        pageable: PageRequest,
    ): List<PostEntity>

    fun existsByCodeAndDeletedAtIsNullAndIdLessThan(code: String, id: String): Boolean

    fun existsByCodeAndDeletedAtIsNullAndIdGreaterThan(code: String, id: String): Boolean

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
        update PostEntity p
        set p.title = :title, p.content = :content, p.updatedAt = :at
        where p.id = :id and p.deletedAt is null
        """,
    )
    fun updateIfActive(
        @Param("id") id: String,
        @Param("title") title: String,
        @Param("content") content: String,
        @Param("at") at: Instant,
    ): Int

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update PostEntity p set p.deletedAt = :at where p.id = :id and p.deletedAt is null")
    fun softDeleteIfActive(@Param("id") id: String, @Param("at") at: Instant): Int

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update PostEntity p set p.commentCount = p.commentCount + 1 where p.id = :id and p.deletedAt is null")
    fun incrementCommentCount(@Param("id") id: String): Int

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update PostEntity p set p.commentCount = p.commentCount - 1 where p.id = :id and p.commentCount > 0")
    fun decrementCommentCount(@Param("id") id: String): Int

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update PostEntity p set p.likeCount = p.likeCount + 1 where p.id = :id and p.deletedAt is null")
    fun incrementLikeCount(@Param("id") id: String): Int

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update PostEntity p set p.likeCount = p.likeCount - 1 where p.id = :id and p.likeCount > 0")
    fun decrementLikeCount(@Param("id") id: String): Int
}

@Repository
class JpaPostStore(
    private val posts: PostJpaRepository,
    private val users: UserStore,
    private val clock: Clock = Clock.systemUTC(),
) : PostStore {
    override fun create(
        id: String,
        code: String,
        authorId: Long,
        title: String,
        content: String,
        quotedEventId: String?,
    ) {
        posts.save(
            PostEntity(
                id = id,
                code = code,
                authorId = authorId,
                title = title,
                content = content,
                quotedEventId = quotedEventId,
                createdAt = clock.instant(),
            ),
        )
    }

    override fun find(id: String): PostRecord? =
        posts.findById(id).orElse(null)?.toRecord()

    override fun findWithAuthor(id: String): PostRowWithAuthor? {
        val post = find(id) ?: return null
        val nickname = users.nicknames(listOf(post.authorId)).getValue(post.authorId)
        return PostRowWithAuthor(post, nickname)
    }

    override fun list(query: PostListQuery): List<PostRowWithAuthor> {
        val ascending = query.direction == CursorDirection.AFTER
        val order = if (ascending) Sort.Direction.ASC else Sort.Direction.DESC
        val page = PageRequest.of(0, query.limit, Sort.by(order, "id"))
        val rows = posts.listRoom(query.code, query.cursor, ascending, page).map { it.toRecord() }
        val nicknames = users.nicknames(rows.map(PostRecord::authorId).distinct())
        return rows.map { PostRowWithAuthor(it, nicknames.getValue(it.authorId)) }
    }

    override fun hasOlderThan(code: String, postId: String): Boolean =
        posts.existsByCodeAndDeletedAtIsNullAndIdLessThan(code, postId)

    override fun hasNewerThan(code: String, postId: String): Boolean =
        posts.existsByCodeAndDeletedAtIsNullAndIdGreaterThan(code, postId)

    override fun updateIfActive(id: String, title: String, content: String, at: Instant): Boolean =
        posts.updateIfActive(id, title, content, at) > 0

    override fun softDeleteIfActive(id: String, at: Instant): Boolean =
        posts.softDeleteIfActive(id, at) > 0

    override fun incrementCommentCount(id: String): Int = posts.incrementCommentCount(id)

    override fun decrementCommentCount(id: String): Int = posts.decrementCommentCount(id)

    override fun incrementLikeCount(id: String): Int = posts.incrementLikeCount(id)

    override fun decrementLikeCount(id: String): Int = posts.decrementLikeCount(id)

    private fun PostEntity.toRecord() = PostRecord(
        id = id.trim(),
        code = code.trim(),
        authorId = authorId,
        title = title,
        content = content,
        quotedEventId = quotedEventId?.trim(),
        likeCount = likeCount,
        commentCount = commentCount,
        createdAt = createdAt,
        updatedAt = updatedAt,
        deletedAt = deletedAt,
    )
}
