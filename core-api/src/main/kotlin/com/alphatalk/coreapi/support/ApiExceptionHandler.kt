package com.alphatalk.coreapi.support

import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.method.annotation.HandlerMethodValidationException
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException

@RestControllerAdvice
class ApiExceptionHandler {
    private val log = LoggerFactory.getLogger(javaClass)

    @ExceptionHandler(RateLimitExceededException::class)
    fun handleRateLimit(e: RateLimitExceededException): ResponseEntity<ApiErrorResponse> =
        ResponseEntity.status(e.code.status)
            .header("Retry-After", e.retryAfterSeconds.toString())
            .body(ApiErrorResponse(ApiErrorBody(e.code.name, e.message, e.detail)))

    @ExceptionHandler(ApiException::class)
    fun handleApi(e: ApiException): ResponseEntity<ApiErrorResponse> =
        respond(e.code, e.message, e.detail)

    @ExceptionHandler(MethodArgumentNotValidException::class)
    fun handleBodyValidation(e: MethodArgumentNotValidException): ResponseEntity<ApiErrorResponse> {
        val field = e.bindingResult.fieldErrors.firstOrNull()
        return respond(
            ErrorCode.VALIDATION_FAILED,
            field?.defaultMessage ?: "요청 값이 올바르지 않습니다",
            field?.let { mapOf("field" to it.field) },
        )
    }

    @ExceptionHandler(HandlerMethodValidationException::class)
    fun handleParameterValidation(e: HandlerMethodValidationException): ResponseEntity<ApiErrorResponse> =
        respond(ErrorCode.VALIDATION_FAILED, "요청 파라미터가 올바르지 않습니다")

    @ExceptionHandler(MethodArgumentTypeMismatchException::class)
    fun handleParameterTypeMismatch(e: MethodArgumentTypeMismatchException): ResponseEntity<ApiErrorResponse> =
        respond(
            ErrorCode.VALIDATION_FAILED,
            "요청 파라미터 형식이 올바르지 않습니다",
            mapOf("field" to e.name),
        )

    @ExceptionHandler(Exception::class)
    fun handleUnexpected(e: Exception): ResponseEntity<ApiErrorResponse> {
        log.error("unhandled exception", e)
        return respond(ErrorCode.INTERNAL, "서버 오류가 발생했습니다")
    }

    private fun respond(
        code: ErrorCode,
        message: String,
        detail: Map<String, Any?>? = null,
    ): ResponseEntity<ApiErrorResponse> =
        ResponseEntity.status(code.status).body(ApiErrorResponse(ApiErrorBody(code.name, message, detail)))
}
