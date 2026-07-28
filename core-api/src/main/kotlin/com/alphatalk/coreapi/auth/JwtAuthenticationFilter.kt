package com.alphatalk.coreapi.auth

import com.alphatalk.auth.ExpiredTokenException
import com.alphatalk.auth.InvalidTokenException
import com.alphatalk.auth.TokenVerifier
import com.alphatalk.coreapi.support.ApiErrorBody
import com.alphatalk.coreapi.support.ApiErrorResponse
import com.alphatalk.coreapi.support.ErrorCode
import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.servlet.FilterChain
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletResponse
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.web.filter.OncePerRequestFilter

class JwtAuthenticationFilter(
    private val verifier: TokenVerifier,
    private val mapper: ObjectMapper,
) : OncePerRequestFilter() {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun doFilterInternal(
        request: HttpServletRequest,
        response: HttpServletResponse,
        filterChain: FilterChain,
    ) {
        val header = request.getHeader(AUTHORIZATION)
        if (header == null || !header.startsWith(BEARER_PREFIX)) {
            filterChain.doFilter(request, response)
            return
        }
        val userId = try {
            verifier.verify(header.removePrefix(BEARER_PREFIX).trim())
        } catch (e: ExpiredTokenException) {
            reject(response, ErrorCode.TOKEN_EXPIRED, "액세스 토큰이 만료되었습니다")
            return
        } catch (e: InvalidTokenException) {
            log.info("access token rejected: {}", e.message)
            reject(response, ErrorCode.UNAUTHORIZED, "인증이 필요합니다")
            return
        }
        SecurityContextHolder.getContext().authentication =
            UsernamePasswordAuthenticationToken(userId, null, emptyList())
        filterChain.doFilter(request, response)
    }

    private fun reject(response: HttpServletResponse, code: ErrorCode, message: String) {
        SecurityContextHolder.clearContext()
        response.status = code.status.value()
        response.contentType = MediaType.APPLICATION_JSON_VALUE
        response.characterEncoding = Charsets.UTF_8.name()
        mapper.writeValue(response.writer, ApiErrorResponse(ApiErrorBody(code.name, message)))
    }

    companion object {
        private const val AUTHORIZATION = "Authorization"
        private const val BEARER_PREFIX = "Bearer "
    }
}
