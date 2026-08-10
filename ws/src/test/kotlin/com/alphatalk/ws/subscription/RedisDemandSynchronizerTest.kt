package com.alphatalk.ws.subscription

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.envelope.DemandUpdated
import com.alphatalk.ws.config.WsProperties
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

@Testcontainers(disabledWithoutDocker = true)
class RedisDemandSynchronizerTest {
    companion object {
        @Container
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)

        private val factory by lazy {
            LettuceConnectionFactory(redis.host, redis.getMappedPort(6379)).apply { afterPropertiesSet() }
        }
        private val template by lazy { StringRedisTemplate(factory) }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            factory.destroy()
        }
    }

    private class FakeSnapshotSource : DemandSnapshotSource {
        @Volatile
        var snapshot = DemandSnapshot.EMPTY

        override fun demandSnapshot(): DemandSnapshot = snapshot
    }

    private val objectMapper = jacksonObjectMapper()
    private val source = FakeSnapshotSource()
    private val trigger = DemandSyncTrigger()
    private val synchronizer by lazy {
        RedisDemandSynchronizer(template, objectMapper, source, trigger, WsProperties())
    }
    private val published = LinkedBlockingQueue<DemandUpdated>()
    private lateinit var listenerContainer: RedisMessageListenerContainer

    @BeforeEach
    fun setUp() {
        template.connectionFactory!!.connection.serverCommands().flushAll()
        listenerContainer = RedisMessageListenerContainer().apply {
            setConnectionFactory(factory)
            addMessageListener(
                { message, _ -> published.add(objectMapper.readValue(message.body, DemandUpdated::class.java)) },
                ChannelTopic(Channels.DEMAND_UPDATED),
            )
            afterPropertiesSet()
            start()
        }
    }

    @AfterEach
    fun stopAll() {
        listenerContainer.destroy()
    }

    private fun quoteHash(): Map<String, String> =
        template.opsForHash<String, String>().entries(Keys.demandQuote(synchronizer.gwId))

    @Test
    fun `sync - 스냅샷을 해시에 기록하고 새 활성 종목의 active true를 발행한다`() {
        source.snapshot = DemandSnapshot(quote = mapOf("005930" to 2), room = mapOf("000660" to 1))

        synchronizer.sync()

        assertThat(quoteHash()).isEqualTo(mapOf("005930" to "2"))
        assertThat(template.opsForHash<String, String>().entries(Keys.demandRoom(synchronizer.gwId)))
            .isEqualTo(mapOf("000660" to "1"))
        assertThat(template.getExpire(Keys.gwAlive(synchronizer.gwId))).isBetween(1L, 15L)
        assertThat(template.getExpire(Keys.demandQuote(synchronizer.gwId))).isBetween(1L, 60L)

        val messages = generateSequence { published.poll(5, TimeUnit.SECONDS) }.take(2).toList()
        assertThat(messages).containsExactlyInAnyOrder(
            DemandUpdated(DemandUpdated.KIND_QUOTE, "005930", active = true, ts = messages.first { it.kind == "quote" }.ts),
            DemandUpdated(DemandUpdated.KIND_ROOM, "000660", active = true, ts = messages.first { it.kind == "room" }.ts),
        )
    }

    @Test
    fun `sync - 카운트만 변한 종목은 발행하지 않고 0이 된 종목만 active false를 발행한다`() {
        source.snapshot = DemandSnapshot(quote = mapOf("005930" to 1, "000660" to 1), room = emptyMap())
        synchronizer.sync()
        repeat(2) { published.poll(5, TimeUnit.SECONDS) }

        source.snapshot = DemandSnapshot(quote = mapOf("005930" to 3), room = emptyMap())
        synchronizer.sync()

        val message = published.poll(5, TimeUnit.SECONDS)
        assertThat(message?.kind).isEqualTo(DemandUpdated.KIND_QUOTE)
        assertThat(message?.code).isEqualTo("000660")
        assertThat(message?.active).isFalse()
        assertThat(published.poll(500, TimeUnit.MILLISECONDS)).isNull()
        assertThat(quoteHash()).isEqualTo(mapOf("005930" to "3"))
    }

    @Test
    fun `sync - 외부에서 어긋난 해시를 스냅샷 전체 재기록으로 자가 치유한다`() {
        source.snapshot = DemandSnapshot(quote = mapOf("005930" to 1), room = emptyMap())
        synchronizer.sync()

        template.opsForHash<String, String>().put(Keys.demandQuote(synchronizer.gwId), "999999", "7")
        template.opsForHash<String, String>().put(Keys.demandQuote(synchronizer.gwId), "005930", "-3")

        synchronizer.sync()

        assertThat(quoteHash()).isEqualTo(mapOf("005930" to "1"))
    }

    @Test
    fun `수요가 비면 해시 키 자체가 사라진다`() {
        source.snapshot = DemandSnapshot(quote = mapOf("005930" to 1), room = emptyMap())
        synchronizer.sync()

        source.snapshot = DemandSnapshot.EMPTY
        synchronizer.sync()

        assertThat(template.hasKey(Keys.demandQuote(synchronizer.gwId))).isFalse()
    }

    @Test
    fun `sync - 레지스트리에 자기 gwId를 등록하고 반복 호출해도 멤버는 하나다`() {
        source.snapshot = DemandSnapshot(quote = mapOf("005930" to 1), room = emptyMap())

        synchronizer.sync()
        synchronizer.sync()
        synchronizer.sync()

        assertThat(template.opsForSet().members(Keys.GW_REGISTRY)).containsExactly(synchronizer.gwId)
    }

    @Test
    fun `sync - 수요가 비어도 레지스트리 등록과 하트비트가 함께 남는다`() {
        source.snapshot = DemandSnapshot.EMPTY

        synchronizer.sync()

        assertThat(template.opsForSet().isMember(Keys.GW_REGISTRY, synchronizer.gwId)).isTrue()
        assertThat(template.hasKey(Keys.gwAlive(synchronizer.gwId))).isTrue()
    }

    @Test
    fun `withdraw - 자기 키를 모두 삭제하고 레지스트리에서도 빠진다`() {
        source.snapshot = DemandSnapshot(quote = mapOf("005930" to 1), room = mapOf("005930" to 1))
        synchronizer.sync()

        synchronizer.withdraw()

        assertThat(template.hasKey(Keys.gwAlive(synchronizer.gwId))).isFalse()
        assertThat(template.hasKey(Keys.demandQuote(synchronizer.gwId))).isFalse()
        assertThat(template.hasKey(Keys.demandRoom(synchronizer.gwId))).isFalse()
        assertThat(template.opsForSet().isMember(Keys.GW_REGISTRY, synchronizer.gwId)).isFalse()
    }

    @Test
    fun `lifecycle - 트리거 요청이 주기를 기다리지 않고 즉시 동기화되고 stop이 키를 정리한다`() {
        val slow = RedisDemandSynchronizer(
            template,
            objectMapper,
            source,
            trigger,
            WsProperties(demand = WsProperties.Demand(heartbeatIntervalSeconds = 3_600)),
        )
        slow.start()
        try {
            await().atMost(Duration.ofSeconds(5)).until { template.hasKey(Keys.gwAlive(slow.gwId)) }

            source.snapshot = DemandSnapshot(quote = mapOf("005930" to 1), room = emptyMap())
            trigger.request()
            await().atMost(Duration.ofSeconds(5)).until {
                template.opsForHash<String, String>().get(Keys.demandQuote(slow.gwId), "005930") == "1"
            }
        } finally {
            slow.stop()
        }
        assertThat(template.hasKey(Keys.gwAlive(slow.gwId))).isFalse()
        assertThat(template.hasKey(Keys.demandQuote(slow.gwId))).isFalse()
    }
}
