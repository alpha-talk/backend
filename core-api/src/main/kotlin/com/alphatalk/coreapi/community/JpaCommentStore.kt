package com.alphatalk.coreapi.community

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Sort
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
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

data class CommentAuthorRow(
    val entity: CommentEntity,
    val nickname: String,
)

interface CommentJpaRepository : JpaRepository<CommentEntity, String> {
    @Query(
        """
        select new com.alphatalk.coreapi.community.CommentAuthorRow(c, u.nickname)
        from CommentEntity c, com.alphatalk.coreapi.auth.UserEntity u
        where u.id = c.authorId and c.postId = :postId and c.deletedAt is null
        """,
    )
    fun listByPost(@Param("postId") postId: String, pageable: PageRequest): List<CommentAuthorRow>

    fun existsByPostIdAndDeletedAtIsNullAndIdGreaterThan(postId: String, id: String): Boolean
}

@Repository
class JpaCommentStore(
    private val comments: CommentJpaRepository,
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

    override fun listFirstPage(postId: String, limit: Int): List<CommentRowWithAuthor> =
        comments.listByPost(postId, PageRequest.of(0, limit, Sort.by(Sort.Direction.ASC, "id")))
            .map { CommentRowWithAuthor(it.entity.toRecord(), it.nickname) }

    override fun hasNewerThan(postId: String, commentId: String): Boolean =
        comments.existsByPostIdAndDeletedAtIsNullAndIdGreaterThan(postId, commentId)

    override fun softDelete(id: String, at: Instant) {
        comments.findById(id).orElse(null)?.let {
            it.deletedAt = at
            comments.save(it)
        }
    }

    private fun CommentEntity.toRecord() = CommentRecord(
        id = id.trim(),
        postId = postId.trim(),
        authorId = authorId,
        content = content,
        createdAt = createdAt,
        deletedAt = deletedAt,
    )
}
