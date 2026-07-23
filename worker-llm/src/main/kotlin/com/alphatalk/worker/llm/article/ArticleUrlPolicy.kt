package com.alphatalk.worker.llm.article

import java.net.URI

class ArticleUrlPolicy(allowedHostSuffixes: List<String>) {
    private val suffixes = allowedHostSuffixes.map { it.lowercase().removePrefix(".") }

    val enabled: Boolean = suffixes.isNotEmpty()

    fun permit(url: String): URI? {
        if (!enabled) return null
        val uri = UrlGuard.safeUrl(url) ?: return null
        return if (hostAllowed(uri.host)) uri else null
    }

    fun permitRedirect(target: URI): URI? {
        val safe = UrlGuard.safeUrl(target.toString()) ?: return null
        return if (hostAllowed(safe.host)) safe else null
    }

    private fun hostAllowed(host: String?): Boolean {
        val h = host?.lowercase() ?: return false
        return suffixes.any { h == it || h.endsWith(".$it") }
    }
}
