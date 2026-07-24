package com.alphatalk.worker.llm.cluster

import java.security.MessageDigest

object TitleNormalizer {
    private val BRACKET_TAG = Regex("[\\[【(](속보|단독|종합|포토|영상|특징주|마감|시황)[)】\\]]")
    private val NON_CONTENT = Regex("[^0-9a-z가-힣]+")

    fun normalize(title: String): String =
        title.lowercase()
            .replace(BRACKET_TAG, " ")
            .replace(NON_CONTENT, " ")
            .trim()
            .replace(Regex("\\s+"), " ")

    fun hash(title: String): String =
        MessageDigest.getInstance("SHA-256")
            .digest(normalize(title).toByteArray())
            .joinToString("") { "%02x".format(it) }
            .take(16)
}
