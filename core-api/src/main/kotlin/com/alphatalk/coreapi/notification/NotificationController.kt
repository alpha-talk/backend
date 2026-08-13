package com.alphatalk.coreapi.notification

import com.alphatalk.coreapi.support.CurrentUser
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1")
class NotificationController(
    private val notifications: NotificationService,
) {
    @GetMapping("/notifications/badge")
    fun badge(): BadgeResponse = notifications.badge(CurrentUser.id())

    @GetMapping("/notifications")
    fun list(
        @RequestParam(required = false) types: String?,
        @RequestParam(required = false) cursor: String?,
        @RequestParam(required = false) limit: Int?,
    ): NotificationPage = notifications.list(CurrentUser.id(), types, cursor, limit)

    @PutMapping("/rooms/{code}/cursor")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun advanceCursor(
        @PathVariable code: String,
        @RequestBody request: CursorAdvanceRequest,
    ) {
        notifications.advanceCursor(CurrentUser.id(), code, request.lastEventId)
    }

    @PostMapping("/notifications/read-all")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun readAll() {
        notifications.readAll(CurrentUser.id())
    }

    @GetMapping("/notifications/opinions")
    fun opinions(
        @RequestParam(required = false) cursor: String?,
        @RequestParam(required = false) limit: Int?,
    ): OpinionPage = notifications.opinions(cursor, limit)

    @PutMapping("/notifications/opinions/cursor")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun advanceOpinionCursor(
        @RequestBody request: CursorAdvanceRequest,
    ) {
        notifications.advanceOpinionCursor(CurrentUser.id(), request.lastEventId)
    }
}
