package com.alphatalk.worker.batch.industry

import org.springframework.core.io.ClassPathResource
import org.springframework.stereotype.Component

data class KsicEntry(val code: String, val name: String) {
    val level: Int get() = code.length
    val parentCode: String? get() = if (code.length > MIN_LEVEL) code.dropLast(1) else null

    private companion object {
        const val MIN_LEVEL = 2
    }
}

@Component
class KsicCatalog(private val resourcePath: String = DEFAULT_RESOURCE) {

    private val entries: List<KsicEntry> by lazy { load() }
    private val byCode: Map<String, KsicEntry> by lazy { entries.associateBy(KsicEntry::code) }

    fun entries(): List<KsicEntry> = entries

    fun contains(code: String): Boolean = code in byCode

    fun sectorCodeOf(indutyCode: String, level: Int): String =
        indutyCode.trim().let { if (it.length >= level) it.take(level) else it }

    fun ancestorNameOf(code: String): String? =
        (code.length - 1 downTo MIN_LEVEL).asSequence()
            .mapNotNull { byCode[code.take(it)]?.name }
            .firstOrNull()

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

    companion object {
        const val VERSION = "KSIC_10"
        const val BASE_LEVEL = 3
        const val MAX_LEVEL = 5
        private const val MIN_LEVEL = 2
        private const val DEFAULT_RESOURCE = "ksic10.csv"
    }
}
