package com.alphatalk.kis.master

import java.nio.charset.Charset

data class KisMember(
    val code: String,
    val name: String,
    val foreign: Boolean,
) {
    val queryCode: String
        get() = code.takeLast(QUERY_CODE_LENGTH)

    val aggregate: Boolean
        get() = code == AGGREGATE_FOREIGN_TOTAL

    companion object {
        const val QUERY_CODE_LENGTH = 3
        const val AGGREGATE_FOREIGN_TOTAL = "99999"
    }
}

data class ParsedMembers(
    val members: List<KisMember>,
    val skippedLines: Int,
)

object KisMemberParser {
    const val FILE_NAME = "memcode"

    private val CP949: Charset = Charset.forName("x-windows-949")
    private const val CODE_LENGTH = 5
    private const val FOREIGN_FLAG = '1'

    fun parse(content: ByteArray): ParsedMembers {
        val rows = splitLines(content).map(::readRow)
        return ParsedMembers(
            members = rows.filterNotNull(),
            skippedLines = rows.count { it == null },
        )
    }

    private fun readRow(line: ByteArray): KisMember? {
        if (line.size <= CODE_LENGTH + 1) return null
        val code = String(line, 0, CODE_LENGTH, CP949).trim()
        if (code.length != CODE_LENGTH || code.any { !it.isDigit() }) return null
        val name = String(line, CODE_LENGTH, line.size - CODE_LENGTH - 1, CP949).trim()
        if (name.isEmpty()) return null
        val flag = line.last().toInt().toChar()
        return KisMember(code = code, name = name, foreign = flag == FOREIGN_FLAG)
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
