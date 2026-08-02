package com.alphatalk.coreapi.community

import com.alphatalk.contracts.envelope.PostData
import com.alphatalk.coreapi.auth.UserStore
import com.alphatalk.coreapi.search.StockCatalog
import com.alphatalk.coreapi.stream.NewStreamEvent
import com.alphatalk.coreapi.stream.StreamEventAppender
import com.alphatalk.coreapi.stream.StreamEventType
import com.alphatalk.coreapi.stream.StreamStore
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.transaction.interceptor.TransactionAspectSupport
import java.time.Clock

sealed interface CreatePostResult {
    data class Created(val response: CreatePostResponse) : CreatePostResult
    data class Replayed(val responseJson: String) : CreatePostResult
    data object UnknownStock : CreatePostResult
    data object QuotedNotFound : CreatePostResult
    data object AuthorMissing : CreatePostResult
    data object KeyActionMismatch : CreatePostResult
}

sealed interface CreateCommentResult {
    data class Created(val response: CreateCommentResponse) : CreateCommentResult
    data class Replayed(val responseJson: String) : CreateCommentResult
    data object PostNotFound : CreateCommentResult
    data object AuthorMissing : CreateCommentResult
    data object KeyActionMismatch : CreateCommentResult
}

enum class MutatePostOutcome { DONE, NOT_FOUND, FORBIDDEN }

enum class LikeOutcome { CHANGED, UNCHANGED, POST_NOT_FOUND }

interface CommunityCommand {
    fun createPost(
        userId: Long,
        code: String,
        title: String,
        content: String,
        quotedEventId: String?,
        postId: String,
        idempotencyKey: String?,
    ): CreatePostResult

    fun createComment(
        userId: Long,
        postId: String,
        content: String,
        commentId: String,
        idempotencyKey: String?,
    ): CreateCommentResult

    fun updatePost(userId: Long, postId: String, title: String?, content: String?): MutatePostOutcome

    fun deletePost(userId: Long, postId: String): MutatePostOutcome

    fun deleteComment(userId: Long, commentId: String): MutatePostOutcome

    fun like(userId: Long, postId: String): LikeOutcome

    fun unlike(userId: Long, postId: String): LikeOutcome

    fun report(userId: Long, postId: String, reason: ReportReason, detail: String?, reportId: String): MutatePostOutcome
}

@Service
class TransactionalCommunityCommand(
    private val posts: PostStore,
    private val comments: CommentStore,
    private val likes: LikeStore,
    private val reports: ReportStore,
    private val users: UserStore,
    private val stocks: StockCatalog,
    private val stream: StreamStore,
    private val appender: StreamEventAppender,
    private val ledger: IdempotencyLedger,
    private val events: ApplicationEventPublisher,
    private val mapper: ObjectMapper,
    private val clock: Clock = Clock.systemUTC(),
) : CommunityCommand {
    @Transactional
    override fun createPost(
        userId: Long,
        code: String,
        title: String,
        content: String,
        quotedEventId: String?,
        postId: String,
        idempotencyKey: String?,
    ): CreatePostResult {
        val author = users.findById(userId) ?: return CreatePostResult.AuthorMissing
        if (!stocks.existsActive(code)) return CreatePostResult.UnknownStock
        if (quotedEventId != null && stream.findInRoom(code, quotedEventId) == null) {
            return CreatePostResult.QuotedNotFound
        }
        if (idempotencyKey != null && !ledger.claim(userId, idempotencyKey, IdempotencyActions.POST_CREATE)) {
            return replayPost(userId, idempotencyKey)
        }
        posts.create(postId, code, userId, title, content, quotedEventId)
        val now = clock.instant()
        appender.append(
            NewStreamEvent(
                eventId = postId,
                code = code,
                type = StreamEventType.POST,
                occurredAt = now,
                source = POST_SOURCE,
                payload = mapper.writeValueAsString(
                    PostStreamPayload(
                        postId = postId,
                        kind = POST_KIND,
                        author = AuthorView(userId, author.nickname),
                        preview = content.take(PREVIEW_LENGTH),
                        likeCount = 0,
                        commentCount = 0,
                    ),
                ),
            ),
        )
        val response = CreatePostResponse(postId = postId, eventId = postId)
        if (idempotencyKey != null) {
            ledger.complete(userId, idempotencyKey, mapper.writeValueAsString(response))
        }
        events.publishEvent(
            PostCommitted(
                code = code,
                eventId = postId,
                data = PostData(
                    kind = POST_KIND,
                    postId = postId,
                    parentId = null,
                    author = PostData.Author(userId, author.nickname),
                    content = content,
                    createdAt = now.toEpochMilli(),
                ),
            ),
        )
        return CreatePostResult.Created(response)
    }

    private fun replayPost(userId: Long, idempotencyKey: String): CreatePostResult {
        val record = checkNotNull(ledger.find(userId, idempotencyKey)) { "claimed idempotency record is missing" }
        if (record.action != IdempotencyActions.POST_CREATE) return CreatePostResult.KeyActionMismatch
        val response = checkNotNull(record.response) { "committed idempotency record has no response" }
        return CreatePostResult.Replayed(response)
    }

    @Transactional
    override fun createComment(
        userId: Long,
        postId: String,
        content: String,
        commentId: String,
        idempotencyKey: String?,
    ): CreateCommentResult {
        val author = users.findById(userId) ?: return CreateCommentResult.AuthorMissing
        val post = posts.find(postId)?.takeIf { it.deletedAt == null } ?: return CreateCommentResult.PostNotFound
        if (idempotencyKey != null && !ledger.claim(userId, idempotencyKey, IdempotencyActions.COMMENT_CREATE)) {
            return replayComment(userId, idempotencyKey)
        }
        comments.create(commentId, postId, userId, content)
        if (posts.incrementCommentCount(postId) == 0) {
            rollback()
            return CreateCommentResult.PostNotFound
        }
        val response = CreateCommentResponse(commentId)
        if (idempotencyKey != null) {
            ledger.complete(userId, idempotencyKey, mapper.writeValueAsString(response))
        }
        events.publishEvent(
            PostCommitted(
                code = post.code,
                eventId = commentId,
                data = PostData(
                    kind = COMMENT_KIND,
                    postId = commentId,
                    parentId = postId,
                    author = PostData.Author(userId, author.nickname),
                    content = content,
                    createdAt = clock.instant().toEpochMilli(),
                ),
            ),
        )
        return CreateCommentResult.Created(response)
    }

    private fun replayComment(userId: Long, idempotencyKey: String): CreateCommentResult {
        val record = checkNotNull(ledger.find(userId, idempotencyKey)) { "claimed idempotency record is missing" }
        if (record.action != IdempotencyActions.COMMENT_CREATE) return CreateCommentResult.KeyActionMismatch
        val response = checkNotNull(record.response) { "committed idempotency record has no response" }
        return CreateCommentResult.Replayed(response)
    }

    @Transactional
    override fun updatePost(userId: Long, postId: String, title: String?, content: String?): MutatePostOutcome {
        val post = posts.find(postId)?.takeIf { it.deletedAt == null } ?: return MutatePostOutcome.NOT_FOUND
        if (post.authorId != userId) return MutatePostOutcome.FORBIDDEN
        val updated = posts.updateIfActive(postId, title, content, clock.instant())
        return if (updated) MutatePostOutcome.DONE else MutatePostOutcome.NOT_FOUND
    }

    @Transactional
    override fun deletePost(userId: Long, postId: String): MutatePostOutcome {
        val post = posts.find(postId) ?: return MutatePostOutcome.NOT_FOUND
        if (post.authorId != userId) return MutatePostOutcome.FORBIDDEN
        if (posts.softDeleteIfActive(postId, clock.instant())) {
            appender.markDeleted(postId)
        }
        return MutatePostOutcome.DONE
    }

    @Transactional
    override fun deleteComment(userId: Long, commentId: String): MutatePostOutcome {
        val comment = comments.find(commentId) ?: return MutatePostOutcome.NOT_FOUND
        if (comment.authorId != userId) return MutatePostOutcome.FORBIDDEN
        if (comments.softDeleteIfActive(commentId, clock.instant())) {
            posts.decrementCommentCount(comment.postId)
        }
        return MutatePostOutcome.DONE
    }

    @Transactional
    override fun like(userId: Long, postId: String): LikeOutcome {
        posts.find(postId)?.takeIf { it.deletedAt == null } ?: return LikeOutcome.POST_NOT_FOUND
        if (!likes.add(postId, userId)) return LikeOutcome.UNCHANGED
        if (posts.incrementLikeCount(postId) == 0) {
            rollback()
            return LikeOutcome.POST_NOT_FOUND
        }
        return LikeOutcome.CHANGED
    }

    @Transactional
    override fun unlike(userId: Long, postId: String): LikeOutcome {
        posts.find(postId)?.takeIf { it.deletedAt == null } ?: return LikeOutcome.POST_NOT_FOUND
        if (!likes.remove(postId, userId)) return LikeOutcome.UNCHANGED
        posts.decrementLikeCount(postId)
        return LikeOutcome.CHANGED
    }

    @Transactional
    override fun report(
        userId: Long,
        postId: String,
        reason: ReportReason,
        detail: String?,
        reportId: String,
    ): MutatePostOutcome {
        posts.find(postId)?.takeIf { it.deletedAt == null } ?: return MutatePostOutcome.NOT_FOUND
        reports.create(reportId, ReportTargetType.POST, postId, userId, reason, detail)
        return MutatePostOutcome.DONE
    }

    private fun rollback() {
        TransactionAspectSupport.currentTransactionStatus().setRollbackOnly()
    }

    companion object {
        const val PREVIEW_LENGTH = 100
        const val POST_SOURCE = "user"
        const val POST_KIND = "post"
        const val COMMENT_KIND = "comment"
    }
}

data class PostStreamPayload(
    val postId: String,
    val kind: String,
    val author: AuthorView,
    val preview: String,
    val likeCount: Int,
    val commentCount: Int,
)
