package com.alphatalk.coreapi.community

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.IdClass
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.io.Serializable
import java.sql.SQLException
import java.time.Clock
import java.time.Instant

data class PostLikeId(
    val postId: String = "",
    val userId: Long = 0,
) : Serializable

@Entity
@Table(name = "post_like")
@IdClass(PostLikeId::class)
class PostLikeEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "post_id", length = 26, columnDefinition = "char(26)")
    var postId: String = "",
    @Id
    @Column(name = "user_id")
    var userId: Long = 0,
    @Column(name = "created_at", nullable = false)
    var createdAt: Instant = Instant.EPOCH,
)

interface PostLikeJpaRepository : JpaRepository<PostLikeEntity, PostLikeId> {
    fun deleteByPostIdAndUserId(postId: String, userId: Long): Long

    fun existsByPostIdAndUserId(postId: String, userId: Long): Boolean
}

@Repository
class JpaLikeStore(
    private val likes: PostLikeJpaRepository,
    private val clock: Clock = Clock.systemUTC(),
) : LikeStore {
    override fun add(postId: String, userId: Long): Boolean {
        if (likes.existsByPostIdAndUserId(postId, userId)) return false
        return try {
            likes.saveAndFlush(PostLikeEntity(postId = postId, userId = userId, createdAt = clock.instant()))
            true
        } catch (e: DataIntegrityViolationException) {
            if ((e.mostSpecificCause as? SQLException)?.sqlState != UNIQUE_VIOLATION_SQL_STATE) throw e
            false
        }
    }

    override fun remove(postId: String, userId: Long): Boolean =
        likes.deleteByPostIdAndUserId(postId, userId) > 0

    override fun exists(postId: String, userId: Long): Boolean =
        likes.existsByPostIdAndUserId(postId, userId)

    companion object {
        private const val UNIQUE_VIOLATION_SQL_STATE = "23505"
    }
}
