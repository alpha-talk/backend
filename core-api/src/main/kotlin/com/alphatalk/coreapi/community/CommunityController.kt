package com.alphatalk.coreapi.community

import com.alphatalk.coreapi.support.CurrentUser
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PatchMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1")
class CommunityController(
    private val community: CommunityService,
) {
    @PostMapping("/rooms/{code}/posts")
    @ResponseStatus(HttpStatus.CREATED)
    fun createPost(
        @PathVariable code: String,
        @Valid @RequestBody request: CreatePostRequest,
        @RequestHeader(name = IDEMPOTENCY_KEY, required = false) idempotencyKey: String?,
    ): CreatePostResponse = community.createPost(CurrentUser.id(), code, request, idempotencyKey)

    @GetMapping("/rooms/{code}/posts")
    fun roomPosts(
        @PathVariable code: String,
        @RequestParam(required = false) cursor: String?,
        @RequestParam(required = false) direction: String?,
        @RequestParam(required = false) limit: Int?,
    ): PostListPage = community.roomPosts(code, cursor, direction, limit)

    @GetMapping("/posts/{postId}")
    fun postDetail(@PathVariable postId: String): PostDetailView =
        community.postDetail(CurrentUser.id(), postId)

    @PatchMapping("/posts/{postId}")
    fun updatePost(
        @PathVariable postId: String,
        @Valid @RequestBody request: UpdatePostRequest,
    ): PostDetailView = community.updatePost(CurrentUser.id(), postId, request)

    @DeleteMapping("/posts/{postId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun deletePost(@PathVariable postId: String) {
        community.deletePost(CurrentUser.id(), postId)
    }

    @PostMapping("/posts/{postId}/comments")
    @ResponseStatus(HttpStatus.CREATED)
    fun createComment(
        @PathVariable postId: String,
        @Valid @RequestBody request: CreateCommentRequest,
        @RequestHeader(name = IDEMPOTENCY_KEY, required = false) idempotencyKey: String?,
    ): CreateCommentResponse = community.createComment(CurrentUser.id(), postId, request, idempotencyKey)

    @GetMapping("/posts/{postId}/comments")
    fun comments(
        @PathVariable postId: String,
        @RequestParam(required = false) cursor: String?,
        @RequestParam(required = false) direction: String?,
        @RequestParam(required = false) limit: Int?,
    ): CommentPage = community.comments(postId, cursor, direction, limit)

    @DeleteMapping("/comments/{commentId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun deleteComment(@PathVariable commentId: String) {
        community.deleteComment(CurrentUser.id(), commentId)
    }

    @PutMapping("/posts/{postId}/like")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun like(@PathVariable postId: String) {
        community.like(CurrentUser.id(), postId)
    }

    @DeleteMapping("/posts/{postId}/like")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun unlike(@PathVariable postId: String) {
        community.unlike(CurrentUser.id(), postId)
    }

    @PostMapping("/posts/{postId}/report")
    @ResponseStatus(HttpStatus.CREATED)
    fun report(
        @PathVariable postId: String,
        @Valid @RequestBody request: CreateReportRequest,
    ): CreateReportResponse = community.report(CurrentUser.id(), postId, request)

    companion object {
        private const val IDEMPOTENCY_KEY = "Idempotency-Key"
    }
}
