package com.alphatalk.coreapi.community

import java.time.Instant

data class CommentRecord(
    val id: String,
    val postId: String,
    val authorId: Long,
    val content: String,
    val createdAt: Instant,
    val deletedAt: Instant?,
)

data class CommentRowWithAuthor(
    val comment: CommentRecord,
    val authorNickname: String,
)

interface CommentStore {
    fun create(id: String, postId: String, authorId: Long, content: String)

    fun find(id: String): CommentRecord?

    fun listFirstPage(postId: String, limit: Int): List<CommentRowWithAuthor>

    fun hasNewerThan(postId: String, commentId: String): Boolean

    fun softDelete(id: String, at: Instant)
}
