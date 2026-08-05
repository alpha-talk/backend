package com.alphatalk.worker.batch.industry

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.ByteArrayInputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.zip.ZipInputStream
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.XMLStreamConstants
import javax.xml.stream.XMLStreamReader

class HttpDartClient(
    private val apiKey: String,
    private val baseUrl: String,
    private val mapper: ObjectMapper,
    private val timeout: Duration = Duration.ofSeconds(60),
) : DartClient {

    private val http: HttpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build()

    override fun corpCodes(): List<DartCorp> {
        val zipped = get("$baseUrl/corpCode.xml?crtfc_key=$apiKey", HttpResponse.BodyHandlers.ofByteArray())
        val xml = unzipSingleEntry(zipped) ?: throw IllegalStateException("corpCode 응답에 XML 엔트리가 없다")
        return parseCorpCodes(xml)
    }

    override fun company(corpCode: String): DartCompany? {
        val body = get(
            "$baseUrl/company.json?crtfc_key=$apiKey&corp_code=$corpCode",
            HttpResponse.BodyHandlers.ofString(),
        )
        val node = mapper.readTree(body)
        if (node.path("status").asText() != OK_STATUS) return null
        return DartCompany(
            corpCode = corpCode,
            stockCode = node.path("stock_code").asText().trim().ifBlank { null },
            indutyCode = node.path("induty_code").asText().trim().ifBlank { null },
            corpName = node.path("corp_name").asText().trim().ifBlank { null },
            corpNameEng = node.path("corp_name_eng").asText().trim().ifBlank { null },
            stockName = node.path("stock_name").asText().trim().ifBlank { null },
            homepage = node.path("hm_url").asText().trim().ifBlank { null },
        )
    }

    private fun <T> get(url: String, handler: HttpResponse.BodyHandler<T>): T {
        val request = HttpRequest.newBuilder(URI.create(url))
            .timeout(timeout)
            .header("User-Agent", USER_AGENT)
            .GET()
            .build()
        val response = http.send(request, handler)
        if (response.statusCode() !in 200..299) {
            throw IllegalStateException("OpenDART 응답 실패: status=${response.statusCode()}")
        }
        return response.body()
    }

    private fun unzipSingleEntry(zipped: ByteArray): ByteArray? =
        ZipInputStream(ByteArrayInputStream(zipped)).use { zip ->
            generateSequence { zip.nextEntry }
                .firstOrNull { !it.isDirectory && it.name.endsWith(".xml", ignoreCase = true) }
                ?.let { zip.readBytes() }
        }

    private fun parseCorpCodes(xml: ByteArray): List<DartCorp> {
        val reader = xmlInputFactory().createXMLStreamReader(ByteArrayInputStream(xml))
        val corps = mutableListOf<DartCorp>()
        var corpCode: String? = null
        var corpName: String? = null
        var stockCode: String? = null
        val text = StringBuilder()
        try {
            while (reader.hasNext()) {
                when (reader.next()) {
                    XMLStreamConstants.START_ELEMENT -> {
                        text.setLength(0)
                        if (reader.localName == LIST_ELEMENT) {
                            corpCode = null
                            corpName = null
                            stockCode = null
                        }
                    }
                    XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA -> text.append(reader.text)
                    XMLStreamConstants.END_ELEMENT -> {
                        val value = text.toString().trim()
                        when (reader.localName) {
                            "corp_code" -> corpCode = value
                            "corp_name" -> corpName = value
                            "stock_code" -> stockCode = value
                            LIST_ELEMENT -> corpCode?.let {
                                corps += DartCorp(it, stockCode?.ifBlank { null }, corpName.orEmpty())
                            }
                        }
                        text.setLength(0)
                    }
                }
            }
        } finally {
            reader.closeQuietly()
        }
        return corps
    }

    private fun XMLStreamReader.closeQuietly() = runCatching { close() }

    private fun xmlInputFactory(): XMLInputFactory = XMLInputFactory.newFactory().apply {
        setProperty(XMLInputFactory.SUPPORT_DTD, false)
        setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
    }

    private companion object {
        const val OK_STATUS = "000"
        const val LIST_ELEMENT = "list"
        const val USER_AGENT = "alphatalk-worker-batch"
    }
}
