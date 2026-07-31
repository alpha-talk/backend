package com.alphatalk.worker.llm.article

import com.alphatalk.worker.llm.config.LlmProperties
import org.springframework.stereotype.Component
import java.net.URI

@Component
class ArticleUrlPolicy(props: LlmProperties) {
    private val suffixes = props.article.allowedHostSuffixes.map { it.lowercase().removePrefix(".") }

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
