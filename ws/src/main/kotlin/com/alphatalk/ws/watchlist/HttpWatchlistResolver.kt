package com.alphatalk.ws.watchlist

import com.alphatalk.contracts.WatchlistSnapshotRequest
import com.alphatalk.contracts.WatchlistSnapshots
import com.alphatalk.contracts.envelope.WatchlistUpdated
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.http.client.JdkClientHttpRequestFactory
import org.springframework.stereotype.Component
import org.springframework.web.client.RestClient
import java.net.http.HttpClient
import java.time.Duration

@Component
class HttpWatchlistResolver(
    builder: RestClient.Builder,
    @Value("\${ALPHATALK_WATCHLIST_CORE_API_URL:http://localhost:8080}") baseUrl: String,
    @param:Value("\${ALPHATALK_WATCHLIST_INTERNAL_KEY:}") private val key: String,
) : WatchlistResolver {
    private val client = builder
        .baseUrl(baseUrl)
        .requestFactory(
            JdkClientHttpRequestFactory(
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build(),
            ).apply { setReadTimeout(Duration.ofSeconds(1)) },
        )
        .build()

    override fun resolve(userIds: List<Long>): List<WatchlistUpdated> {
        check(key.isNotBlank()) { "Watchlist internal key is missing" }
        return requireNotNull(
            client.post()
                .uri(WatchlistSnapshots.PATH)
                .header(WatchlistSnapshots.KEY_HEADER, key)
                .contentType(MediaType.APPLICATION_JSON)
                .body(WatchlistSnapshotRequest(userIds))
                .retrieve()
                .body(object : ParameterizedTypeReference<List<WatchlistUpdated>>() {}),
        )
    }
}
