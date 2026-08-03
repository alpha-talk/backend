package com.alphatalk.coreapi.auth

import com.alphatalk.coreapi.support.CurrentUser
import com.alphatalk.coreapi.support.RateLimited
import com.alphatalk.coreapi.support.RateLimits
import jakarta.servlet.http.HttpServletRequest
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1/auth")
class AuthController(
    private val auth: AuthService,
) {
    @PostMapping("/signup")
    @ResponseStatus(HttpStatus.CREATED)
    fun signup(@Valid @RequestBody request: SignupRequest): SignupResponse =
        SignupResponse(auth.signup(request))

    @PostMapping("/login")
    @RateLimited(
        action = RateLimits.LOGIN,
        limit = RateLimits.LOGIN_PER_MINUTE,
        key = RateLimits.LOGIN_KEY,
    )
    fun login(@Valid @RequestBody request: LoginRequest, http: HttpServletRequest): TokenPair =
        auth.login(request)

    @PostMapping("/refresh")
    fun refresh(@Valid @RequestBody request: RefreshRequest): TokenPair =
        auth.refresh(request)

    @PostMapping("/logout")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun logout() {
        auth.logout(CurrentUser.id())
    }
}
