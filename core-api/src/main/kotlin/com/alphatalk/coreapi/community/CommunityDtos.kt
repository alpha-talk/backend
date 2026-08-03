package com.alphatalk.coreapi.community

import com.alphatalk.coreapi.stream.PageInfo
import com.fasterxml.jackson.databind.JsonNode
import jakarta.validation.constraints.Size

data class CreatePostRequest(
    @field:Size(min = 1, max = 100, message = "title은 1~100자여야 합니다")
    val title: String,
    @field:Size(min = 1, max = 2000, message = "content는 1~2000자여야 합니다")
    val content: String,
    val quotedEventId: String? = null,
)

data class CreatePostResponse(
    val postId: String,
    val eventId: String,
)

data class UpdatePostRequest(
    @field:Size(min = 1, max = 100, message = "title은 1~100자여야 합니다")
    val title: String?,
    @field:Size(min = 1, max = 2000, message = "content는 1~2000자여야 합니다")
    val content: String?,
)

data class CreateCommentRequest(
    @field:Size(min = 1, max = 1000, message = "content는 1~1000자여야 합니다")
    val content: String,
)

data class CreateCommentResponse(
    val commentId: String,
)

data class CreateReportRequest(
    val reason: String?,
    @field:Size(max = 500, message = "detail은 500자 이하여야 합니다")
    val detail: String? = null,
)

data class CreateReportResponse(
    val reportId: String,
)

data class AuthorView(
    val id: Long,
    val nickname: String,
)

data class PostSummaryView(
    val postId: String,
    val code: String,
    val author: AuthorView,
    val title: String,
    val preview: String,
    val likeCount: Int,
    val commentCount: Int,
    val createdAt: Long,
)

data class PostListPage(
    val items: List<PostSummaryView>,
    val pageInfo: PageInfo,
)

data class CommentView(
    val commentId: String,
    val author: AuthorView,
    val content: String,
    val createdAt: Long,
)

data class CommentPage(
    val items: List<CommentView>,
    val pageInfo: PageInfo,
)

data class QuotedView(
    val eventId: String,
    val type: String,
    val payload: JsonNode,
)

data class PostDetailView(
    val postId: String,
    val code: String,
    val author: AuthorView,
    val title: String,
    val content: String,
    val likeCount: Int,
    val likedByMe: Boolean,
    val commentCount: Int,
    val quoted: QuotedView?,
    val comments: CommentPage,
    val createdAt: Long,
    val updatedAt: Long?,
    val deleted: Boolean,
)
