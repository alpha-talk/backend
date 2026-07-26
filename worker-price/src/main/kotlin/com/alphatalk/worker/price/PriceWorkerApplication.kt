package com.alphatalk.worker.price

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class PriceWorkerApplication

fun main(args: Array<String>) {
    runApplication<PriceWorkerApplication>(*args)
}
