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

data class CommentListQuery(
    val postId: String,
    val cursor: String?,
    val ascending: Boolean,
    val limit: Int,
)

interface CommentStore {
    fun create(id: String, postId: String, authorId: Long, content: String)

    fun find(id: String): CommentRecord?

    fun list(query: CommentListQuery): List<CommentRowWithAuthor>

    fun hasOlderThan(postId: String, commentId: String): Boolean

    fun hasNewerThan(postId: String, commentId: String): Boolean

    fun softDeleteIfActive(id: String, at: Instant): Boolean
}
