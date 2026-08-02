package com.alphatalk.coreapi.community

import com.alphatalk.coreapi.stream.CursorDirection
import java.time.Instant

data class PostRecord(
    val id: String,
    val code: String,
    val authorId: Long,
    val title: String,
    val content: String,
    val quotedEventId: String?,
    val likeCount: Int,
    val commentCount: Int,
    val createdAt: Instant,
    val updatedAt: Instant?,
    val deletedAt: Instant?,
)

data class PostRowWithAuthor(
    val post: PostRecord,
    val authorNickname: String,
)

data class PostListQuery(
    val code: String,
    val cursor: String?,
    val direction: CursorDirection,
    val limit: Int,
)

interface PostStore {
    fun create(
        id: String,
        code: String,
        authorId: Long,
        title: String,
        content: String,
        quotedEventId: String?,
    )

    fun find(id: String): PostRecord?

    fun findWithAuthor(id: String): PostRowWithAuthor?

    fun list(query: PostListQuery): List<PostRowWithAuthor>

    fun hasOlderThan(code: String, postId: String): Boolean

    fun hasNewerThan(code: String, postId: String): Boolean

    fun update(id: String, title: String, content: String, at: Instant)

    fun softDelete(id: String, at: Instant)

    fun incrementCommentCount(id: String): Int

    fun decrementCommentCount(id: String): Int

    fun incrementLikeCount(id: String): Int

    fun decrementLikeCount(id: String): Int
}
