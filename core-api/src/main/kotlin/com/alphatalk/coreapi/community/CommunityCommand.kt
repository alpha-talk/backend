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
import java.time.Clock

enum class CreatePostOutcome { CREATED, UNKNOWN_STOCK, QUOTED_NOT_FOUND, AUTHOR_MISSING }

enum class CreateCommentOutcome { CREATED, POST_NOT_FOUND, AUTHOR_MISSING }

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
    ): CreatePostOutcome

    fun createComment(userId: Long, postId: String, content: String, commentId: String): CreateCommentOutcome

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
    ): CreatePostOutcome {
        val author = users.findById(userId) ?: return CreatePostOutcome.AUTHOR_MISSING
        if (!stocks.existsActive(code)) return CreatePostOutcome.UNKNOWN_STOCK
        if (quotedEventId != null && stream.findInRoom(code, quotedEventId) == null) {
            return CreatePostOutcome.QUOTED_NOT_FOUND
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
        return CreatePostOutcome.CREATED
    }

    @Transactional
    override fun createComment(
        userId: Long,
        postId: String,
        content: String,
        commentId: String,
    ): CreateCommentOutcome {
        val author = users.findById(userId) ?: return CreateCommentOutcome.AUTHOR_MISSING
        val post = posts.find(postId)?.takeIf { it.deletedAt == null } ?: return CreateCommentOutcome.POST_NOT_FOUND
        comments.create(commentId, postId, userId, content)
        posts.incrementCommentCount(postId)
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
        return CreateCommentOutcome.CREATED
    }

    @Transactional
    override fun updatePost(userId: Long, postId: String, title: String?, content: String?): MutatePostOutcome {
        val post = posts.find(postId)?.takeIf { it.deletedAt == null } ?: return MutatePostOutcome.NOT_FOUND
        if (post.authorId != userId) return MutatePostOutcome.FORBIDDEN
        posts.update(postId, title ?: post.title, content ?: post.content, clock.instant())
        return MutatePostOutcome.DONE
    }

    @Transactional
    override fun deletePost(userId: Long, postId: String): MutatePostOutcome {
        val post = posts.find(postId) ?: return MutatePostOutcome.NOT_FOUND
        if (post.authorId != userId) return MutatePostOutcome.FORBIDDEN
        if (post.deletedAt != null) return MutatePostOutcome.DONE
        posts.softDelete(postId, clock.instant())
        appender.markDeleted(postId)
        return MutatePostOutcome.DONE
    }

    @Transactional
    override fun deleteComment(userId: Long, commentId: String): MutatePostOutcome {
        val comment = comments.find(commentId) ?: return MutatePostOutcome.NOT_FOUND
        if (comment.authorId != userId) return MutatePostOutcome.FORBIDDEN
        if (comment.deletedAt != null) return MutatePostOutcome.DONE
        comments.softDelete(commentId, clock.instant())
        posts.decrementCommentCount(comment.postId)
        return MutatePostOutcome.DONE
    }

    @Transactional
    override fun like(userId: Long, postId: String): LikeOutcome {
        posts.find(postId)?.takeIf { it.deletedAt == null } ?: return LikeOutcome.POST_NOT_FOUND
        if (!likes.add(postId, userId)) return LikeOutcome.UNCHANGED
        posts.incrementLikeCount(postId)
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
