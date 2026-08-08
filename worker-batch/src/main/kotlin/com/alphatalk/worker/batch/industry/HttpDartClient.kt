package com.alphatalk.worker.batch.industry

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.ByteArrayInputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.LocalDate
import java.time.format.DateTimeFormatter
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

    override fun periodicDisclosures(begin: LocalDate, end: LocalDate): List<DartDisclosure> {
        val disclosures = mutableListOf<DartDisclosure>()
        var page = 1
        while (true) {
            val body = get(
                "$baseUrl/list.json?crtfc_key=$apiKey" +
                    "&bgn_de=${begin.format(DateTimeFormatter.BASIC_ISO_DATE)}" +
                    "&end_de=${end.format(DateTimeFormatter.BASIC_ISO_DATE)}" +
                    "&pblntf_ty=$PERIODIC_DISCLOSURE_TYPE&page_no=$page&page_count=$LIST_PAGE_SIZE",
                HttpResponse.BodyHandlers.ofString(),
            )
            val node = mapper.readTree(body)
            when (val status = node.path("status").asText()) {
                OK_STATUS -> Unit
                NO_DATA_STATUS -> return disclosures
                else -> throw DartApiException(status, "OpenDART list 조회 실패: status=$status ${node.path("message").asText()}")
            }
            node.path("list").forEach { item ->
                disclosures += DartDisclosure(
                    corpCode = item.path("corp_code").asText().trim(),
                    stockCode = item.path("stock_code").asText().trim().ifBlank { null },
                    reportName = item.path("report_nm").asText().trim(),
                    receiptDate = item.path("rcept_dt").asText().trim(),
                )
            }
            val totalPage = node.path("total_page").asInt(1)
            if (page >= totalPage) return disclosures
            page += 1
        }
    }

    override fun financialAccounts(corpCode: String, year: Int, reprtCode: String, fsDiv: String): List<DartFinancialAccount> {
        val body = get(
            "$baseUrl/fnlttSinglAcntAll.json?crtfc_key=$apiKey" +
                "&corp_code=$corpCode&bsns_year=$year&reprt_code=$reprtCode&fs_div=$fsDiv",
            HttpResponse.BodyHandlers.ofString(),
        )
        val node = mapper.readTree(body)
        when (val status = node.path("status").asText()) {
            OK_STATUS -> Unit
            NO_DATA_STATUS -> return emptyList()
            else -> throw DartApiException(status, "OpenDART 재무제표 조회 실패: status=$status ${node.path("message").asText()}")
        }
        return node.path("list").map { item ->
            DartFinancialAccount(
                sjDiv = item.path("sj_div").asText().trim(),
                accountId = item.path("account_id").asText().trim().takeIf { it.isNotEmpty() && it != "-" },
                accountName = item.path("account_nm").asText().trim(),
                accountDetail = item.path("account_detail").asText().trim().takeIf { it.isNotEmpty() && it != "-" },
                amount = item.path("thstrm_amount").asText().trim().replace(",", "").toLongOrNull(),
                currency = item.path("currency").asText().trim().ifBlank { null },
            )
        }
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
        const val PERIODIC_DISCLOSURE_TYPE = "A"
        const val LIST_PAGE_SIZE = 100
    }
}
