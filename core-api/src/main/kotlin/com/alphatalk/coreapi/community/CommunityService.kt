package com.alphatalk.coreapi.community

import com.alphatalk.coreapi.stream.CursorDirection
import com.alphatalk.coreapi.stream.PageInfo
import com.alphatalk.coreapi.stream.StreamStore
import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import com.alphatalk.coreapi.support.RateLimitExceededException
import com.alphatalk.coreapi.support.RateLimiter
import org.springframework.stereotype.Service
import java.time.Duration

@Service
class CommunityService(
    private val command: CommunityCommand,
    private val posts: PostStore,
    private val comments: CommentStore,
    private val likes: LikeStore,
    private val stream: StreamStore,
    private val ids: CommunityIdGenerator,
    private val rateLimiter: RateLimiter,
    private val idempotency: IdempotencyCache,
) {
    fun createPost(userId: Long, rawCode: String, request: CreatePostRequest, idempotencyKey: String?): CreatePostResponse {
        val code = validCode(rawCode)
        val quotedEventId = request.quotedEventId?.let(::validUlid)
        val key = idempotencyKey?.let(::validIdempotencyKey)
        key?.let { idempotency.find(userId, it, CreatePostResponse::class.java) }?.let { return it }
        checkRate(POST_ACTION, userId, POST_LIMIT_PER_MINUTE)
        val postId = ids.next()
        val outcome = command.createPost(userId, code, request.title, request.content, quotedEventId, postId)
        val response = when (outcome) {
            CreatePostOutcome.CREATED -> CreatePostResponse(postId = postId, eventId = postId)
            CreatePostOutcome.UNKNOWN_STOCK ->
                throw ApiException(ErrorCode.NOT_FOUND, "존재하지 않는 종목입니다", mapOf("code" to code))

            CreatePostOutcome.QUOTED_NOT_FOUND ->
                throw ApiException(
                    ErrorCode.VALIDATION_FAILED,
                    "인용한 이벤트가 이 방에 없습니다",
                    mapOf("field" to "quotedEventId"),
                )

            CreatePostOutcome.AUTHOR_MISSING -> throw unauthorized()
        }
        key?.let { idempotency.store(userId, it, response) }
        return response
    }

    fun createComment(userId: Long, rawPostId: String, request: CreateCommentRequest, idempotencyKey: String?): CreateCommentResponse {
        val postId = validUlid(rawPostId)
        val key = idempotencyKey?.let(::validIdempotencyKey)
        key?.let { idempotency.find(userId, it, CreateCommentResponse::class.java) }?.let { return it }
        checkRate(COMMENT_ACTION, userId, COMMENT_LIMIT_PER_MINUTE)
        val commentId = ids.next()
        val response = when (command.createComment(userId, postId, request.content, commentId)) {
            CreateCommentOutcome.CREATED -> CreateCommentResponse(commentId)
            CreateCommentOutcome.POST_NOT_FOUND -> throw postNotFound(postId)
            CreateCommentOutcome.AUTHOR_MISSING -> throw unauthorized()
        }
        key?.let { idempotency.store(userId, it, response) }
        return response
    }

    fun postDetail(userId: Long, rawPostId: String): PostDetailView {
        val postId = validUlid(rawPostId)
        val row = posts.findWithAuthor(postId) ?: throw postNotFound(postId)
        val post = row.post
        val deleted = post.deletedAt != null
        val commentRows = comments.listFirstPage(postId, COMMENT_PAGE_SIZE)
        val commentViews = commentRows.map {
            CommentView(
                commentId = it.comment.id,
                author = AuthorView(it.comment.authorId, it.authorNickname),
                content = it.comment.content,
                createdAt = it.comment.createdAt.toEpochMilli(),
            )
        }
        val commentPage = CommentPage(
            items = commentViews,
            pageInfo = PageInfo(
                oldest = commentViews.firstOrNull()?.commentId,
                newest = commentViews.lastOrNull()?.commentId,
                hasMoreBefore = false,
                hasMoreAfter = commentViews.lastOrNull()
                    ?.let { comments.hasNewerThan(postId, it.commentId) } == true,
            ),
        )
        val quoted = if (deleted) {
            null
        } else {
            post.quotedEventId
                ?.let { stream.findInRoom(post.code, it) }
                ?.let { QuotedView(eventId = it.eventId, type = it.type, payload = it.payload) }
        }
        return PostDetailView(
            postId = post.id,
            code = post.code,
            author = AuthorView(post.authorId, row.authorNickname),
            title = if (deleted) "" else post.title,
            content = if (deleted) "" else post.content,
            likeCount = post.likeCount,
            likedByMe = likes.exists(postId, userId),
            commentCount = post.commentCount,
            quoted = quoted,
            comments = commentPage,
            createdAt = post.createdAt.toEpochMilli(),
            updatedAt = post.updatedAt?.toEpochMilli(),
            deleted = deleted,
        )
    }

    fun roomPosts(rawCode: String, cursor: String?, direction: String?, limit: Int?): PostListPage {
        val code = validCode(rawCode)
        val validCursor = cursor?.trim().orEmpty().ifEmpty { null }?.let(::validUlid)
        val validDirection = validDirection(direction)
        if (validDirection == CursorDirection.AFTER && validCursor == null) {
            throw ApiException(
                ErrorCode.VALIDATION_FAILED,
                "direction=after에는 cursor가 필요합니다",
                mapOf("field" to "cursor"),
            )
        }
        val rows = posts.list(
            PostListQuery(
                code = code,
                cursor = validCursor,
                direction = validDirection,
                limit = validLimit(limit),
            ),
        )
        val items = rows.map {
            PostSummaryView(
                postId = it.post.id,
                code = it.post.code,
                author = AuthorView(it.post.authorId, it.authorNickname),
                title = it.post.title,
                preview = it.post.content.take(TransactionalCommunityCommand.PREVIEW_LENGTH),
                likeCount = it.post.likeCount,
                commentCount = it.post.commentCount,
                createdAt = it.post.createdAt.toEpochMilli(),
            )
        }
        if (items.isEmpty()) {
            return PostListPage(items, PageInfo(oldest = null, newest = null, hasMoreBefore = false, hasMoreAfter = false))
        }
        val postIds = items.map(PostSummaryView::postId)
        val oldest = postIds.min()
        val newest = postIds.max()
        return PostListPage(
            items,
            PageInfo(
                oldest = oldest,
                newest = newest,
                hasMoreBefore = posts.hasOlderThan(code, oldest),
                hasMoreAfter = if (validCursor == null && validDirection == CursorDirection.BEFORE) {
                    false
                } else {
                    posts.hasNewerThan(code, newest)
                },
            ),
        )
    }

    fun updatePost(userId: Long, rawPostId: String, request: UpdatePostRequest): PostDetailView {
        val postId = validUlid(rawPostId)
        if (request.title == null && request.content == null) {
            throw ApiException(ErrorCode.VALIDATION_FAILED, "수정할 내용이 없습니다")
        }
        when (command.updatePost(userId, postId, request.title, request.content)) {
            MutatePostOutcome.DONE -> Unit
            MutatePostOutcome.NOT_FOUND -> throw postNotFound(postId)
            MutatePostOutcome.FORBIDDEN -> throw forbidden()
        }
        return postDetail(userId, postId)
    }

    fun deletePost(userId: Long, rawPostId: String) {
        val postId = validUlid(rawPostId)
        when (command.deletePost(userId, postId)) {
            MutatePostOutcome.DONE -> Unit
            MutatePostOutcome.NOT_FOUND -> throw postNotFound(postId)
            MutatePostOutcome.FORBIDDEN -> throw forbidden()
        }
    }

    fun deleteComment(userId: Long, rawCommentId: String) {
        val commentId = validUlid(rawCommentId)
        when (command.deleteComment(userId, commentId)) {
            MutatePostOutcome.DONE -> Unit
            MutatePostOutcome.NOT_FOUND ->
                throw ApiException(ErrorCode.NOT_FOUND, "존재하지 않는 댓글입니다", mapOf("commentId" to commentId))

            MutatePostOutcome.FORBIDDEN -> throw forbidden()
        }
    }

    fun like(userId: Long, rawPostId: String) {
        val postId = validUlid(rawPostId)
        checkRate(LIKE_ACTION, userId, LIKE_LIMIT_PER_MINUTE)
        if (command.like(userId, postId) == LikeOutcome.POST_NOT_FOUND) throw postNotFound(postId)
    }

    fun unlike(userId: Long, rawPostId: String) {
        val postId = validUlid(rawPostId)
        checkRate(LIKE_ACTION, userId, LIKE_LIMIT_PER_MINUTE)
        if (command.unlike(userId, postId) == LikeOutcome.POST_NOT_FOUND) throw postNotFound(postId)
    }

    fun report(userId: Long, rawPostId: String, request: CreateReportRequest): CreateReportResponse {
        val postId = validUlid(rawPostId)
        val reason = request.reason?.let(ReportReason::fromToken)
            ?: throw ApiException(
                ErrorCode.VALIDATION_FAILED,
                "reason은 SPAM, ABUSE, MANIPULATION, ETC 중 하나여야 합니다",
                mapOf("field" to "reason"),
            )
        val reportId = ids.next()
        when (command.report(userId, postId, reason, request.detail?.trim()?.ifEmpty { null }, reportId)) {
            MutatePostOutcome.DONE -> Unit
            MutatePostOutcome.NOT_FOUND -> throw postNotFound(postId)
            MutatePostOutcome.FORBIDDEN -> throw forbidden()
        }
        return CreateReportResponse(reportId)
    }

    private fun checkRate(action: String, userId: Long, limit: Int) {
        val decision = rateLimiter.tryAcquire(action, userId.toString(), limit, Duration.ofMinutes(1))
        if (!decision.allowed) throw RateLimitExceededException(action, decision.retryAfterSeconds)
    }

    private fun postNotFound(postId: String) =
        ApiException(ErrorCode.NOT_FOUND, "존재하지 않는 글입니다", mapOf("postId" to postId))

    private fun forbidden() = ApiException(ErrorCode.FORBIDDEN, "본인이 작성한 글만 수정하거나 삭제할 수 있습니다")

    private fun unauthorized() = ApiException(ErrorCode.UNAUTHORIZED, "인증이 필요합니다")

    private fun validCode(code: String): String {
        if (!CODE_PATTERN.matches(code)) {
            throw ApiException(ErrorCode.VALIDATION_FAILED, "종목 코드는 6자리 숫자여야 합니다", mapOf("field" to "code"))
        }
        return code
    }

    private fun validUlid(value: String): String {
        val trimmed = value.trim()
        if (!ULID_PATTERN.matches(trimmed)) {
            throw ApiException(ErrorCode.VALIDATION_FAILED, "식별자 형식이 올바르지 않습니다", mapOf("value" to trimmed))
        }
        return trimmed
    }

    private fun validIdempotencyKey(key: String): String {
        val trimmed = key.trim()
        if (!ULID_PATTERN.matches(trimmed)) {
            throw ApiException(
                ErrorCode.VALIDATION_FAILED,
                "Idempotency-Key는 ULID여야 합니다",
                mapOf("field" to "Idempotency-Key"),
            )
        }
        return trimmed
    }

    private fun validDirection(direction: String?): CursorDirection {
        val trimmed = direction?.trim().orEmpty()
        if (trimmed.isEmpty()) return CursorDirection.BEFORE
        return CursorDirection.fromToken(trimmed)
            ?: throw ApiException(
                ErrorCode.VALIDATION_FAILED,
                "direction은 before 또는 after여야 합니다",
                mapOf("field" to "direction"),
            )
    }

    private fun validLimit(limit: Int?): Int {
        val value = limit ?: DEFAULT_LIMIT
        if (value < MIN_LIMIT || value > MAX_LIMIT) {
            throw ApiException(
                ErrorCode.VALIDATION_FAILED,
                "limit은 $MIN_LIMIT~$MAX_LIMIT 사이여야 합니다",
                mapOf("field" to "limit"),
            )
        }
        return value
    }

    companion object {
        const val DEFAULT_LIMIT = 50
        const val MIN_LIMIT = 1
        const val MAX_LIMIT = 100
        const val COMMENT_PAGE_SIZE = 50
        const val POST_ACTION = "post"
        const val COMMENT_ACTION = "comment"
        const val LIKE_ACTION = "like"
        const val POST_LIMIT_PER_MINUTE = 5
        const val COMMENT_LIMIT_PER_MINUTE = 10
        const val LIKE_LIMIT_PER_MINUTE = 60
        private val CODE_PATTERN = Regex("^\\d{6}$")
        private val ULID_PATTERN = Regex("^[0-9A-HJKMNP-TV-Z]{26}$")
    }
}
