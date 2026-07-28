package com.alphatalk.kis.master

import com.alphatalk.kis.KisClientException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.zip.ZipInputStream

class KisMasterClient(
    private val baseUrl: String = DEFAULT_BASE_URL,
    private val http: HttpClient = HttpClient.newHttpClient(),
    private val timeout: Duration = Duration.ofMinutes(2),
) {
    fun download(market: KisMarket): ByteArray = download(market.fileName)

    fun downloadSectors(): ByteArray = download(KisSectorParser.FILE_NAME)

    private fun download(fileName: String): ByteArray {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl/$fileName.mst.zip"))
            .timeout(timeout)
            .GET()
            .build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofByteArray())
        if (response.statusCode() != 200) {
            throw KisClientException("master download failed: file=$fileName status=${response.statusCode()}")
        }
        return unzipSingleEntry(response.body(), fileName)
    }

    private fun unzipSingleEntry(archive: ByteArray, fileName: String): ByteArray {
        ZipInputStream(archive.inputStream()).use { zip ->
            var entry = zip.nextEntry
            while (entry != null) {
                if (!entry.isDirectory && entry.name.endsWith(MASTER_SUFFIX)) {
                    return zip.readBytes()
                }
                entry = zip.nextEntry
            }
        }
        throw KisClientException("master archive has no $MASTER_SUFFIX entry: file=$fileName")
    }

    companion object {
        const val DEFAULT_BASE_URL = "https://new.real.download.dws.co.kr/common/master"
        private const val MASTER_SUFFIX = ".mst"
    }
}
