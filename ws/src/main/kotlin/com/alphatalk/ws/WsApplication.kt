package com.alphatalk.ws

import com.alphatalk.ws.watchlist.WatchlistSnapshotClient
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.cloud.openfeign.EnableFeignClients
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
@EnableFeignClients(clients = [WatchlistSnapshotClient::class])
class WsApplication

fun main(args: Array<String>) {
    runApplication<WsApplication>(*args)
}
