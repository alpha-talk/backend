package com.alphatalk.kis.master

import java.nio.charset.Charset

data class KisSector(
    val code: String,
    val name: String,
)

data class ParsedSectors(
    val sectors: List<KisSector>,
    val skippedLines: Int,
) {
    val isComplete: Boolean
        get() = skippedLines == 0
}

object KisSectorParser {
    const val FILE_NAME = "idxcode"

    private val CP949: Charset = Charset.forName("x-windows-949")
    private const val LINE_LENGTH = 45
    private const val CODE_TO = 5

    fun parse(content: ByteArray): ParsedSectors {
        val rows = splitLines(content).map(::readRow)
        return ParsedSectors(
            sectors = rows.filterNotNull()
                .filter { it.name.isNotEmpty() }
                .map { KisSector(it.code, it.name) },
            skippedLines = rows.count { it == null },
        )
    }

    private data class Row(val code: String, val name: String)

    private fun readRow(line: ByteArray): Row? {
        if (line.size != LINE_LENGTH) return null
        val code = String(line, 0, CODE_TO, CP949).trim()
        if (code.isEmpty()) return null
        return Row(code, String(line, CODE_TO, LINE_LENGTH - CODE_TO, CP949).trim())
    }

    private fun splitLines(content: ByteArray): List<ByteArray> {
        val lines = mutableListOf<ByteArray>()
        var start = 0
        for (i in content.indices) {
            if (content[i] == '\n'.code.toByte()) {
                addIfNotBlank(lines, content, start, i)
                start = i + 1
            }
        }
        addIfNotBlank(lines, content, start, content.size)
        return lines
    }

    private fun addIfNotBlank(lines: MutableList<ByteArray>, content: ByteArray, from: Int, toExclusive: Int) {
        if (toExclusive <= from) return
        val end = if (content[toExclusive - 1] == '\r'.code.toByte()) toExclusive - 1 else toExclusive
        if (end <= from) return
        lines += content.copyOfRange(from, end)
    }
}
