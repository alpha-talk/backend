package com.alphatalk.coreapi.community

import com.alphatalk.coreapi.stream.CursorDirection
import com.alphatalk.coreapi.stream.StreamItem
import com.alphatalk.coreapi.stream.StreamEventType
import com.alphatalk.coreapi.stream.StreamQuery
import com.alphatalk.coreapi.stream.StreamStore
import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class CommunityServiceTest {
    private val mapper = jacksonObjectMapper()

    private class RecordingCommand(private val ledger: InMemoryLedger? = null) : CommunityCommand {
        var createPostResult: CreatePostResult? = null
        var createCommentResult: CreateCommentResult? = null
        var mutateOutcome = MutatePostOutcome.DONE
        var likeOutcome = LikeOutcome.CHANGED
        val calls = mutableListOf<String>()
        private val commandMapper = jacksonObjectMapper()

        override fun createPost(
            userId: Long,
            code: String,
            title: String,
            content: String,
            quotedEventId: String?,
            postId: String,
            idempotencyKey: String?,
        ): CreatePostResult {
            calls += "createPost:$code:$postId:$quotedEventId"
            createPostResult?.let { return it }
            val response = CreatePostResponse(postId, postId)
            idempotencyKey?.let {
                ledger?.claim(userId, it, IdempotencyActions.POST_CREATE)
                ledger?.complete(userId, it, commandMapper.writeValueAsString(response))
            }
            return CreatePostResult.Created(response)
        }

        override fun createComment(
            userId: Long,
            postId: String,
            content: String,
            commentId: String,
            idempotencyKey: String?,
        ): CreateCommentResult {
            calls += "createComment:$postId:$commentId"
            return createCommentResult ?: CreateCommentResult.Created(CreateCommentResponse(commentId))
        }

        override fun updatePost(userId: Long, postId: String, title: String?, content: String?): MutatePostOutcome {
            calls += "updatePost:$postId"
            return mutateOutcome
        }

        override fun deletePost(userId: Long, postId: String): MutatePostOutcome {
            calls += "deletePost:$postId"
            return mutateOutcome
        }

        override fun deleteComment(userId: Long, commentId: String): MutatePostOutcome {
            calls += "deleteComment:$commentId"
            return mutateOutcome
        }

        override fun like(userId: Long, postId: String): LikeOutcome {
            calls += "like:$postId"
            return likeOutcome
        }

        override fun unlike(userId: Long, postId: String): LikeOutcome {
            calls += "unlike:$postId"
            return likeOutcome
        }

        override fun report(
            userId: Long,
            postId: String,
            reason: ReportReason,
            detail: String?,
            reportId: String,
        ): MutatePostOutcome {
            calls += "report:$postId:${reason.name}"
            return mutateOutcome
        }
    }

    private class InMemoryLedger : IdempotencyLedger {
        val rows = mutableMapOf<String, IdempotencyRecord>()

        override fun find(userId: Long, key: String): IdempotencyRecord? = rows["$userId:$key"]

        override fun claim(userId: Long, key: String, action: String): Boolean {
            if (rows.containsKey("$userId:$key")) return false
            rows["$userId:$key"] = IdempotencyRecord(action, null)
            return true
        }

        override fun complete(userId: Long, key: String, response: String) {
            val id = "$userId:$key"
            rows[id] = requireNotNull(rows[id]).copy(response = response)
        }
    }

    private class FakePostStore(
        private val rows: MutableMap<String, PostRowWithAuthor> = mutableMapOf(),
    ) : PostStore {
        var listResult: List<PostRowWithAuthor> = emptyList()
        var older = false
        var newer = false

        fun put(row: PostRowWithAuthor) {
            rows[row.post.id] = row
        }

        override fun create(id: String, code: String, authorId: Long, title: String, content: String, quotedEventId: String?) =
            throw UnsupportedOperationException()

        override fun find(id: String) = rows[id]?.post

        override fun findWithAuthor(id: String) = rows[id]

        override fun list(query: PostListQuery) = listResult

        override fun hasOlderThan(code: String, postId: String) = older

        override fun hasNewerThan(code: String, postId: String) = newer

        override fun updateIfActive(id: String, title: String?, content: String?, at: Instant) = throw UnsupportedOperationException()

        override fun softDeleteIfActive(id: String, at: Instant) = throw UnsupportedOperationException()

        override fun incrementCommentCount(id: String) = 0
        override fun decrementCommentCount(id: String) = 0
        override fun incrementLikeCount(id: String) = 0
        override fun decrementLikeCount(id: String) = 0
    }

    private class FakeCommentStore(
        private val pages: Map<String, List<CommentRowWithAuthor>> = emptyMap(),
    ) : CommentStore {
        var lastQuery: CommentListQuery? = null
        var newer = false

        override fun create(id: String, postId: String, authorId: Long, content: String) = throw UnsupportedOperationException()
        override fun find(id: String): CommentRecord? = null

        override fun list(query: CommentListQuery): List<CommentRowWithAuthor> {
            lastQuery = query
            return pages[query.postId].orEmpty().take(query.limit)
        }

        override fun hasOlderThan(postId: String, commentId: String) = false
        override fun hasNewerThan(postId: String, commentId: String) = newer
        override fun softDeleteIfActive(id: String, at: Instant) = throw UnsupportedOperationException()
    }

    private class FakeLikeStore(private val liked: Set<Pair<String, Long>> = emptySet()) : LikeStore {
        override fun add(postId: String, userId: Long) = true
        override fun remove(postId: String, userId: Long) = true
        override fun exists(postId: String, userId: Long) = (postId to userId) in liked
    }

    private class FakeStreamStore(private val known: Map<Pair<String, String>, StreamItem> = emptyMap()) : StreamStore {
        override fun find(query: StreamQuery): List<StreamItem> = emptyList()
        override fun hasOlderThan(code: String, eventId: String, types: List<StreamEventType>) = false
        override fun hasNewerThan(code: String, eventId: String, types: List<StreamEventType>) = false
        override fun findInRoom(code: String, eventId: String) = known[code to eventId]
    }

    private class SequenceIds(vararg ids: String) : CommunityIdGenerator {
        private val queue = ArrayDeque(ids.toList())
        override fun next(): String = queue.removeFirst()
    }

    private fun post(
        id: String = POST_ID,
        code: String = "005930",
        authorId: Long = 1,
        deletedAt: Instant? = null,
        quotedEventId: String? = null,
    ) = PostRowWithAuthor(
        PostRecord(
            id = id,
            code = code,
            authorId = authorId,
            title = "제목",
            content = "본문",
            quotedEventId = quotedEventId,
            likeCount = 3,
            commentCount = 1,
            createdAt = Instant.parse("2026-07-30T00:00:00Z"),
            updatedAt = null,
            deletedAt = deletedAt,
        ),
        "민균",
    )

    private fun service(
        command: CommunityCommand = RecordingCommand(),
        posts: PostStore = FakePostStore(),
        comments: CommentStore = FakeCommentStore(),
        likes: LikeStore = FakeLikeStore(),
        stream: StreamStore = FakeStreamStore(),
        ids: CommunityIdGenerator = SequenceIds(POST_ID, COMMENT_ID),
        ledger: IdempotencyLedger = InMemoryLedger(),
    ) = CommunityService(command, posts, comments, likes, stream, ids, ledger, mapper)

    @Test
    fun `글을 만들면 postId와 eventId가 같은 ULID다`() {
        val response = service().createPost(1, "005930", CreatePostRequest("제목", "본문"), null)

        assertEquals(POST_ID, response.postId)
        assertEquals(POST_ID, response.eventId)
    }

    @Test
    fun `기록된 Idempotency-Key 재요청은 원장 응답을 재생하고 커맨드를 다시 실행하지 않는다`() {
        val ledger = InMemoryLedger()
        val command = RecordingCommand(ledger)
        val target = service(
            command = command,
            ids = SequenceIds(POST_ID, "01JA000000000000000000000B"),
            ledger = ledger,
        )

        val first = target.createPost(1, "005930", CreatePostRequest("제목", "본문"), IDEM_KEY)
        val replay = target.createPost(1, "005930", CreatePostRequest("제목", "본문"), IDEM_KEY)

        assertEquals(first, replay)
        assertEquals(1, command.calls.size)
    }

    @Test
    fun `같은 키가 다른 요청 종류에 쓰이면 409다`() {
        val ledger = InMemoryLedger().apply {
            claim(1, IDEM_KEY, IdempotencyActions.COMMENT_CREATE)
            complete(1, IDEM_KEY, """{"commentId":"01JA0000000000000000000002"}""")
        }

        val e = assertFailsWith<ApiException> {
            service(ledger = ledger).createPost(1, "005930", CreatePostRequest("제목", "본문"), IDEM_KEY)
        }

        assertEquals(ErrorCode.CONFLICT, e.code)
    }

    @Test
    fun `커맨드가 원장 재생을 돌려주면 그 응답을 그대로 준다`() {
        val command = RecordingCommand().apply {
            createPostResult = CreatePostResult.Replayed("""{"postId":"$POST_ID","eventId":"$POST_ID"}""")
        }

        val response = service(command = command).createPost(1, "005930", CreatePostRequest("제목", "본문"), IDEM_KEY)

        assertEquals(POST_ID, response.postId)
    }

    @Test
    fun `Idempotency-Key가 ULID가 아니면 400이다`() {
        val e = assertFailsWith<ApiException> {
            service().createPost(1, "005930", CreatePostRequest("제목", "본문"), "not-a-ulid")
        }

        assertEquals(ErrorCode.VALIDATION_FAILED, e.code)
    }

    @Test
    fun `없는 방에 글을 쓰면 404고 인용이 깨지면 400이다`() {
        val unknownStock = RecordingCommand().apply { createPostResult = CreatePostResult.UnknownStock }
        assertEquals(
            ErrorCode.NOT_FOUND,
            assertFailsWith<ApiException> {
                service(command = unknownStock).createPost(1, "005930", CreatePostRequest("제목", "본문"), null)
            }.code,
        )

        val badQuote = RecordingCommand().apply { createPostResult = CreatePostResult.QuotedNotFound }
        val e = assertFailsWith<ApiException> {
            service(command = badQuote)
                .createPost(1, "005930", CreatePostRequest("제목", "본문", quotedEventId = QUOTED_ID), null)
        }
        assertEquals(ErrorCode.VALIDATION_FAILED, e.code)
    }

    @Test
    fun `댓글을 만들면 commentId를 준다`() {
        val response = service(ids = SequenceIds(COMMENT_ID))
            .createComment(1, POST_ID, CreateCommentRequest("댓글"), null)

        assertEquals(COMMENT_ID, response.commentId)
    }

    @Test
    fun `글 상세는 인용 이벤트와 내가 눌렀는지 여부를 담는다`() {
        val posts = FakePostStore().apply { put(post(quotedEventId = QUOTED_ID)) }
        val quoted = StreamItem(
            eventId = QUOTED_ID,
            code = "005930",
            type = "NEWS",
            occurredAt = 1719500000000,
            source = "hankyung",
            payload = mapper.readTree("""{"title":"인용 뉴스"}"""),
        )

        val detail = service(
            posts = posts,
            likes = FakeLikeStore(liked = setOf(POST_ID to 1L)),
            stream = FakeStreamStore(mapOf(("005930" to QUOTED_ID) to quoted)),
        ).postDetail(1, POST_ID)

        assertTrue(detail.likedByMe)
        assertEquals(QUOTED_ID, detail.quoted?.eventId)
        assertEquals("NEWS", detail.quoted?.type)
        assertFalse(detail.deleted)
    }

    @Test
    fun `삭제된 글 상세는 본문을 감추고 deleted를 표시한다`() {
        val posts = FakePostStore().apply {
            put(post(deletedAt = Instant.parse("2026-07-31T00:00:00Z"), quotedEventId = QUOTED_ID))
        }

        val detail = service(posts = posts).postDetail(1, POST_ID)

        assertTrue(detail.deleted)
        assertEquals("", detail.title)
        assertEquals("", detail.content)
        assertNull(detail.quoted)
    }

    @Test
    fun `없는 글 상세는 404다`() {
        val e = assertFailsWith<ApiException> { service().postDetail(1, POST_ID) }

        assertEquals(ErrorCode.NOT_FOUND, e.code)
    }

    @Test
    fun `댓글 목록의 기본 방향은 처음부터 오름차순이다`() {
        val posts = FakePostStore().apply { put(post()) }
        val comments = FakeCommentStore()

        service(posts = posts, comments = comments).comments(POST_ID, null, null, null)

        assertEquals(CommentListQuery(POST_ID, null, ascending = true, limit = 50), comments.lastQuery)
    }

    @Test
    fun `댓글 목록은 커서와 방향을 검증하고 없는 글은 404다`() {
        val posts = FakePostStore().apply { put(post()) }

        assertEquals(
            ErrorCode.VALIDATION_FAILED,
            assertFailsWith<ApiException> {
                service(posts = posts).comments(POST_ID, "bad-cursor", null, null)
            }.code,
        )
        assertEquals(
            ErrorCode.NOT_FOUND,
            assertFailsWith<ApiException> { service().comments(POST_ID, null, null, null) }.code,
        )
    }

    @Test
    fun `방 글 목록은 미리보기를 100자로 자르고 pageInfo를 채운다`() {
        val longContent = "가".repeat(500)
        val posts = FakePostStore().apply {
            listResult = listOf(
                PostRowWithAuthor(post().post.copy(id = "01JA000000000000000000000C", content = longContent), "민균"),
                PostRowWithAuthor(post().post.copy(id = "01JA000000000000000000000A"), "민균"),
            )
            older = true
        }

        val page = service(posts = posts).roomPosts("005930", null, null, null)

        assertEquals(100, page.items.first().preview.length)
        assertEquals("01JA000000000000000000000A", page.pageInfo.oldest)
        assertEquals("01JA000000000000000000000C", page.pageInfo.newest)
        assertTrue(page.pageInfo.hasMoreBefore)
        assertFalse(page.pageInfo.hasMoreAfter)
    }

    @Test
    fun `after 방향에는 cursor가 필요하다`() {
        val e = assertFailsWith<ApiException> { service().roomPosts("005930", null, "after", null) }

        assertEquals(ErrorCode.VALIDATION_FAILED, e.code)
    }

    @Test
    fun `타인의 글 수정과 삭제는 403이다`() {
        val command = RecordingCommand().apply { mutateOutcome = MutatePostOutcome.FORBIDDEN }

        assertEquals(
            ErrorCode.FORBIDDEN,
            assertFailsWith<ApiException> {
                service(command = command).updatePost(2, POST_ID, UpdatePostRequest("새 제목", null))
            }.code,
        )
        assertEquals(
            ErrorCode.FORBIDDEN,
            assertFailsWith<ApiException> { service(command = command).deletePost(2, POST_ID) }.code,
        )
    }

    @Test
    fun `수정할 내용이 없으면 400이다`() {
        val e = assertFailsWith<ApiException> { service().updatePost(1, POST_ID, UpdatePostRequest(null, null)) }

        assertEquals(ErrorCode.VALIDATION_FAILED, e.code)
    }

    @Test
    fun `신고 사유가 목록에 없으면 400이다`() {
        val e = assertFailsWith<ApiException> {
            service().report(1, POST_ID, CreateReportRequest(reason = "WHATEVER"))
        }

        assertEquals(ErrorCode.VALIDATION_FAILED, e.code)
    }

    @Test
    fun `신고가 접수되면 reportId를 준다`() {
        val command = RecordingCommand()

        val response = service(command = command, ids = SequenceIds(REPORT_ID))
            .report(1, POST_ID, CreateReportRequest(reason = "spam", detail = " 도배 "))

        assertEquals(REPORT_ID, response.reportId)
        assertNotNull(command.calls.singleOrNull { it == "report:$POST_ID:SPAM" })
    }

    companion object {
        private const val POST_ID = "01JA0000000000000000000001"
        private const val COMMENT_ID = "01JA0000000000000000000002"
        private const val REPORT_ID = "01JA0000000000000000000003"
        private const val QUOTED_ID = "01J9Z800000000000000000009"
        private const val IDEM_KEY = "01JA00000000000000000000ZZ"
    }
}
