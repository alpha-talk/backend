package com.alphatalk.coreapi.subscription

import com.alphatalk.coreapi.support.CurrentUser
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

data class WatchlistResponse(val items: List<WatchlistItem>)

@RestController
@RequestMapping("/api/v1/watchlist")
class WatchlistController(
    private val watchlist: WatchlistService,
) {
    @GetMapping
    fun list(): WatchlistResponse = WatchlistResponse(watchlist.list(CurrentUser.id()))

    @PutMapping("/{code}")
    fun subscribe(@PathVariable code: String): ResponseEntity<Void> {
        val created = watchlist.subscribe(CurrentUser.id(), code)
        return ResponseEntity.status(if (created) HttpStatus.CREATED else HttpStatus.OK).build()
    }

    @DeleteMapping("/{code}")
    fun unsubscribe(@PathVariable code: String): ResponseEntity<Void> {
        watchlist.unsubscribe(CurrentUser.id(), code)
        return ResponseEntity.noContent().build()
    }
}
