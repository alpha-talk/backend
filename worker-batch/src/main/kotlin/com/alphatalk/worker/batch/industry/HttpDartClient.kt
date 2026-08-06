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
        val body = get("$baseUrl/corpCode.xml?crtfc_key=$apiKey", HttpResponse.BodyHandlers.ofByteArray())
        val xml = unzipSingleEntry(body)
            ?: throw DartApiException(statusOfXml(body), "corpCode 응답이 zip이 아니다: ${statusOfXml(body)}")
        return parseCorpCodes(xml)
    }

    override fun company(corpCode: String): DartCompany? {
        val body = get(
            "$baseUrl/company.json?crtfc_key=$apiKey&corp_code=$corpCode",
            HttpResponse.BodyHandlers.ofString(),
        )
        val node = mapper.readTree(body)
        when (val status = node.path("status").asText()) {
            OK_STATUS -> Unit
            NO_DATA_STATUS -> return null
            else -> throw DartApiException(status, "OpenDART company 조회 실패: status=$status ${node.path("message").asText()}")
        }
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
            throw DartApiException(response.statusCode().toString(), "OpenDART HTTP ${response.statusCode()}")
        }
        return response.body()
    }

    private fun statusOfXml(body: ByteArray): String =
        Regex("<status>(\\d+)</status>").find(String(body, Charsets.UTF_8).take(512))?.groupValues?.get(1)
            ?: UNKNOWN_STATUS

    private fun unzipSingleEntry(zipped: ByteArray): ByteArray? =
        runCatching {
            ZipInputStream(ByteArrayInputStream(zipped)).use { zip ->
                generateSequence { zip.nextEntry }
                    .firstOrNull { !it.isDirectory && it.name.endsWith(".xml", ignoreCase = true) }
                    ?.let { zip.readBytes() }
            }
        }.getOrNull()

    private fun parseCorpCodes(xml: ByteArray): List<DartCorp> {
        val reader = xmlInputFactory().createXMLStreamReader(ByteArrayInputStream(xml))
        val corps = mutableListOf<DartCorp>()
        var corpCode: String? = null
        var corpName: String? = null
        var stockCode: String? = null
        var modifyDate: String? = null
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
                            modifyDate = null
                        }
                    }
                    XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA -> text.append(reader.text)
                    XMLStreamConstants.END_ELEMENT -> {
                        val value = text.toString().trim()
                        when (reader.localName) {
                            "corp_code" -> corpCode = value
                            "corp_name" -> corpName = value
                            "stock_code" -> stockCode = value
                            "modify_date" -> modifyDate = value
                            LIST_ELEMENT -> corpCode?.let {
                                corps += DartCorp(
                                    corpCode = it,
                                    stockCode = stockCode?.ifBlank { null },
                                    corpName = corpName.orEmpty(),
                                    modifyDate = modifyDate?.ifBlank { null },
                                )
                            }
                        }
                        text.setLength(0)
                    }
                }
            }
        } finally {
            runCatching { reader.close() }
        }
        return corps
    }

    private fun xmlInputFactory(): XMLInputFactory = XMLInputFactory.newFactory().apply {
        setProperty(XMLInputFactory.SUPPORT_DTD, false)
        setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false)
    }

    private companion object {
        const val OK_STATUS = "000"
        const val NO_DATA_STATUS = "013"
        const val UNKNOWN_STATUS = "unknown"
        const val LIST_ELEMENT = "list"
        const val USER_AGENT = "alphatalk-worker-batch"
    }
}
