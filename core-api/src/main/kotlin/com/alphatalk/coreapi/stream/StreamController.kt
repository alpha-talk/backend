package com.alphatalk.coreapi.stream

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1/rooms")
class StreamController(
    private val stream: StreamService,
) {
    @GetMapping("/{code}/stream")
    fun stream(
        @PathVariable code: String,
        @RequestParam(required = false) cursor: String?,
        @RequestParam(required = false) direction: String?,
        @RequestParam(required = false) limit: Int?,
        @RequestParam(required = false) types: String?,
    ): StreamPage = stream.read(code, cursor, direction, limit, types)

    @GetMapping("/{code}/quote")
    fun quote(@PathVariable code: String): QuoteResponse = stream.quote(code)
}
