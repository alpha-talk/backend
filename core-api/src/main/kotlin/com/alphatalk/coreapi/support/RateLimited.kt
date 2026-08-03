package com.alphatalk.coreapi.support

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class RateLimited(
    val action: String,
    val limit: Int,
    val windowSeconds: Long = 60,
    val key: String = "",
)

object RateLimits {
    const val POST = "post"
    const val POST_PER_MINUTE = 5

    const val COMMENT = "comment"
    const val COMMENT_PER_MINUTE = 10

    const val LIKE = "like"
    const val LIKE_PER_MINUTE = 60

    const val LOGIN = "login"
    const val LOGIN_PER_MINUTE = 10
    const val LOGIN_KEY = "#http.remoteAddr + ':' + #request.email"
}
