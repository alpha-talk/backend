package com.alphatalk.worker.batch.config

import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.task.TaskSchedulingAutoConfiguration
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler
import kotlin.test.assertTrue

class SchedulerPoolSizeTest {
    @Test
    fun `스케줄러 풀은 단일 스레드가 아니다 - 늘어진 DART 잡이 그날의 KIS 잡을 굶기지 않게`() {
        ApplicationContextRunner()
            .withInitializer(ConfigDataApplicationContextInitializer())
            .withConfiguration(AutoConfigurations.of(TaskSchedulingAutoConfiguration::class.java))
            .withUserConfiguration(SchedulingEnabled::class.java)
            .run { context ->
                val corePoolSize = context.getBean(ThreadPoolTaskScheduler::class.java).scheduledThreadPoolExecutor.corePoolSize
                assertTrue(corePoolSize > 1, "spring.task.scheduling.pool.size=$corePoolSize")
            }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableScheduling
    class SchedulingEnabled
}
