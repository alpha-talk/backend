package com.alphatalk.worker.ingest.normalize

import java.net.URI
import java.security.MessageDigest

object ArticleNormalizer {
    private val TRACKING_PARAMS = setOf(
        "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid",
    )

    fun normalizeUrl(url: String): String {
        val trimmed = url.trim()
        val uri = runCatching { URI(trimmed) }.getOrElse { return trimmed }
        val scheme = uri.scheme?.lowercase() ?: return trimmed
        val host = uri.host?.lowercase() ?: return trimmed
        val port = if (uri.port == -1) "" else ":${uri.port}"
        val query = uri.rawQuery
            ?.split("&")
            ?.filter { it.substringBefore("=").lowercase() !in TRACKING_PARAMS }
            ?.takeIf { it.isNotEmpty() }
            ?.joinToString("&")
        return buildString {
            append(scheme).append("://").append(host).append(port).append(uri.rawPath.orEmpty())
            if (query != null) append("?").append(query)
        }
    }

    fun sourceId(source: String, providedId: String?, normalizedUrl: String): String =
        if (providedId != null) "$source:$providedId" else "$source:${sha256Hex(normalizedUrl).take(16)}"

    private fun sha256Hex(value: String): String =
        MessageDigest.getInstance("SHA-256")
            .digest(value.toByteArray())
            .joinToString("") { "%02x".format(it) }
}
