package com.alphatalk.worker.ingest

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class IngestWorkerApplication

fun main(args: Array<String>) {
    runApplication<IngestWorkerApplication>(*args)
}
