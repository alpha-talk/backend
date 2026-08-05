package com.alphatalk.worker.batch.industry

import org.springframework.core.io.ClassPathResource
import org.springframework.stereotype.Component

data class KsicEntry(val code: String, val name: String) {
    val level: Int get() = code.length
}

@Component
class KsicCatalog(private val resourcePath: String = DEFAULT_RESOURCE) {

    private val entries: List<KsicEntry> by lazy { load() }

    fun entries(): List<KsicEntry> = entries

    fun ancestorNameOf(code: String): String? =
        (code.length - 1 downTo 2).asSequence()
            .map { code.take(it) }
            .mapNotNull { prefix -> entries.firstOrNull { it.code == prefix }?.name }
            .firstOrNull()

    fun groupCodeOf(indutyCode: String): String = truncate(indutyCode, GROUP_LEVEL)

    fun subGroupCodeOf(indutyCode: String): String = truncate(indutyCode, SUB_GROUP_LEVEL)

    private fun truncate(indutyCode: String, level: Int): String =
        indutyCode.trim().let { if (it.length >= level) it.take(level) else it }

    private fun load(): List<KsicEntry> {
        val resource = ClassPathResource(resourcePath)
        check(resource.exists()) { "KSIC 분류표 리소스가 없다: $resourcePath" }
        return resource.inputStream.bufferedReader(Charsets.UTF_8).useLines { lines ->
            lines.drop(1)
                .mapNotNull { parse(it) }
                .distinctBy(KsicEntry::code)
                .toList()
        }
    }

    private fun parse(line: String): KsicEntry? {
        val fields = splitCsv(line)
        if (fields.size < 2) return null
        val code = fields[0].trim()
        val name = fields[1].trim()
        if (code.isEmpty() || name.isEmpty() || !code.all(Char::isDigit)) return null
        return KsicEntry(code, name)
    }

    private fun splitCsv(line: String): List<String> {
        val fields = mutableListOf<String>()
        val current = StringBuilder()
        var quoted = false
        line.forEach { ch ->
            when {
                ch == '"' -> quoted = !quoted
                ch == ',' && !quoted -> {
                    fields += current.toString()
                    current.setLength(0)
                }
                else -> current.append(ch)
            }
        }
        fields += current.toString()
        return fields
    }

    private companion object {
        const val DEFAULT_RESOURCE = "ksic10.csv"
        const val GROUP_LEVEL = 3
        const val SUB_GROUP_LEVEL = 4
    }
}
