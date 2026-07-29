package com.alphatalk.coreapi.support

import com.fasterxml.jackson.annotation.JsonInclude
import org.springframework.http.HttpStatus

enum class ErrorCode(val status: HttpStatus) {
    VALIDATION_FAILED(HttpStatus.BAD_REQUEST),
    UNAUTHORIZED(HttpStatus.UNAUTHORIZED),
    TOKEN_EXPIRED(HttpStatus.UNAUTHORIZED),
    FORBIDDEN(HttpStatus.FORBIDDEN),
    NOT_FOUND(HttpStatus.NOT_FOUND),
    CONFLICT(HttpStatus.CONFLICT),
    DUPLICATE(HttpStatus.CONFLICT),
    LIMIT_EXCEEDED(HttpStatus.UNPROCESSABLE_ENTITY),
    RATE_LIMITED(HttpStatus.TOO_MANY_REQUESTS),
    INTERNAL(HttpStatus.INTERNAL_SERVER_ERROR),
}

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ApiErrorBody(
    val code: String,
    val message: String,
    val detail: Map<String, Any?>? = null,
)

data class ApiErrorResponse(val error: ApiErrorBody)

open class ApiException(
    val code: ErrorCode,
    override val message: String,
    val detail: Map<String, Any?>? = null,
) : RuntimeException(message)
