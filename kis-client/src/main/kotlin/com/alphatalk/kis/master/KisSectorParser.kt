package com.alphatalk.kis.master

import java.nio.charset.Charset

data class KisSector(
    val code: String,
    val name: String,
)

object KisSectorParser {
    const val FILE_NAME = "idxcode"

    private val CP949: Charset = Charset.forName("x-windows-949")
    private const val LINE_LENGTH = 45
    private const val CODE_TO = 5

    fun parse(content: ByteArray): List<KisSector> =
        content.split('\n'.code.toByte())
            .mapNotNull(::parseLine)

    private fun parseLine(line: ByteArray): KisSector? {
        val trimmed = if (line.isNotEmpty() && line.last() == '\r'.code.toByte()) {
            line.copyOfRange(0, line.size - 1)
        } else {
            line
        }
        if (trimmed.size != LINE_LENGTH) return null
        val code = String(trimmed, 0, CODE_TO, CP949).trim()
        val name = String(trimmed, CODE_TO, LINE_LENGTH - CODE_TO, CP949).trim()
        if (code.isEmpty() || name.isEmpty()) return null
        return KisSector(code, name)
    }

    private fun ByteArray.split(separator: Byte): List<ByteArray> {
        val parts = mutableListOf<ByteArray>()
        var start = 0
        for (i in indices) {
            if (this[i] == separator) {
                if (i > start) parts += copyOfRange(start, i)
                start = i + 1
            }
        }
        if (start < size) parts += copyOfRange(start, size)
        return parts
    }
}
