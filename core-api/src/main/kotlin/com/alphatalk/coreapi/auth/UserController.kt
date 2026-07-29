package com.alphatalk.coreapi.auth

import com.alphatalk.coreapi.support.CurrentUser
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1/users")
class UserController(
    private val auth: AuthService,
) {
    @GetMapping("/me")
    fun me(): MeResponse = auth.me(CurrentUser.id())
}
