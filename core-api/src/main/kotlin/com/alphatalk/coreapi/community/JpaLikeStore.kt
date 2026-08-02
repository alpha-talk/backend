package com.alphatalk.coreapi.community

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.IdClass
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable
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
    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
        insert into PostLikeEntity (postId, userId, createdAt)
        values (:postId, :userId, :at)
        on conflict do nothing
        """,
    )
    fun insertIfAbsent(
        @Param("postId") postId: String,
        @Param("userId") userId: Long,
        @Param("at") at: Instant,
    ): Int

    fun deleteByPostIdAndUserId(postId: String, userId: Long): Long

    fun existsByPostIdAndUserId(postId: String, userId: Long): Boolean
}

@Repository
class JpaLikeStore(
    private val likes: PostLikeJpaRepository,
    private val clock: Clock = Clock.systemUTC(),
) : LikeStore {
    override fun add(postId: String, userId: Long): Boolean =
        likes.insertIfAbsent(postId, userId, clock.instant()) > 0

    override fun remove(postId: String, userId: Long): Boolean =
        likes.deleteByPostIdAndUserId(postId, userId) > 0

    @Transactional(readOnly = true)
    override fun exists(postId: String, userId: Long): Boolean =
        likes.existsByPostIdAndUserId(postId, userId)
}
