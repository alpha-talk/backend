package com.alphatalk.worker.batch

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class BatchWorkerApplication

fun main(args: Array<String>) {
    runApplication<BatchWorkerApplication>(*args)
}
