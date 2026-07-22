package com.alphatalk.worker.llm

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class LlmWorkerApplication

fun main(args: Array<String>) {
    runApplication<LlmWorkerApplication>(*args)
}
