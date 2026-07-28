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
    fun download(market: KisMarket): ByteArray {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl/${market.fileName}.mst.zip"))
            .timeout(timeout)
            .GET()
            .build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofByteArray())
        if (response.statusCode() != 200) {
            throw KisClientException("master download failed: market=$market status=${response.statusCode()}")
        }
        return unzipSingleEntry(response.body(), market)
    }

    private fun unzipSingleEntry(archive: ByteArray, market: KisMarket): ByteArray {
        ZipInputStream(archive.inputStream()).use { zip ->
            var entry = zip.nextEntry
            while (entry != null) {
                if (!entry.isDirectory && entry.name.endsWith(MASTER_SUFFIX)) {
                    return zip.readBytes()
                }
                entry = zip.nextEntry
            }
        }
        throw KisClientException("master archive has no $MASTER_SUFFIX entry: market=$market")
    }

    companion object {
        const val DEFAULT_BASE_URL = "https://new.real.download.dws.co.kr/common/master"
        private const val MASTER_SUFFIX = ".mst"
    }
}
