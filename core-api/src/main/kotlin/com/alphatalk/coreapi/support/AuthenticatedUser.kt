package com.alphatalk.coreapi.support

import org.springframework.security.core.context.SecurityContextHolder

object CurrentUser {
    fun id(): Long =
        SecurityContextHolder.getContext().authentication?.principal as? Long
            ?: throw ApiException(ErrorCode.UNAUTHORIZED, "인증이 필요합니다")
}
