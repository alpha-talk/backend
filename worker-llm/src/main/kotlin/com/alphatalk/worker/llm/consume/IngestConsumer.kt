package com.alphatalk.worker.llm.consume

import com.alphatalk.contracts.Queues
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.llm.cluster.ClusterContendedException
import com.alphatalk.worker.llm.config.LlmProperties
import com.alphatalk.worker.llm.enrich.DigestProcessor
import com.alphatalk.worker.llm.enrich.NewsProcessor
import io.lettuce.core.RedisBusyException
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.stream.Consumer
import org.springframework.data.redis.connection.stream.MapRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.connection.stream.StreamReadOptions
import org.springframework.data.redis.connection.stream.StreamRecords
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.connection.RedisStreamCommands.XClaimOptions
import org.springframework.stereotype.Component
import java.lang.management.ManagementFactory

@Component
class IngestConsumer(
    private val redis: StringRedisTemplate,
    private val news: NewsProcessor,
    private val digest: DigestProcessor,
    private val meters: MeterRegistry,
    props: LlmProperties,
    val consumerName: String = ManagementFactory.getRuntimeMXBean().name,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val block = props.consumerBlock
    private val batch = props.consumerBatch
    private val poisonMaxDeliveries = props.poisonMaxDeliveries
    private val claimIdle = props.claimIdle

    fun ensureGroup() {
        runCatching {
            redis.execute { connection ->
                connection.streamCommands().xGroupCreate(
                    Queues.INGEST.toByteArray(),
                    Queues.INGEST_GROUP_LLM,
                    ReadOffset.from("0"),
                    true,
                )
            }
        }.onFailure { failure ->
            val busy = generateSequence<Throwable>(failure) { it.cause }
                .any { it is RedisBusyException && it.message?.startsWith("BUSYGROUP") == true }
            if (!busy) throw failure
        }
    }

    fun pollOnce(): Int {
        val records = redis.opsForStream<String, String>().read(
            Consumer.from(Queues.INGEST_GROUP_LLM, consumerName),
            StreamReadOptions.empty().count(batch.toLong()).block(block),
            StreamOffset.create(Queues.INGEST, ReadOffset.lastConsumed()),
        ).orEmpty()
        return records.count(::handle)
    }

    fun claimStale(): Int {
        val pending = runCatching {
            redis.opsForStream<String, String>()
                .pending(Queues.INGEST, Queues.INGEST_GROUP_LLM, Range.unbounded<String>(), 100)
        }.getOrNull() ?: return 0

        var handled = 0
        for (message in pending) {
            if (message.elapsedTimeSinceLastDelivery < claimIdle) continue
            val claimed = redis.opsForStream<String, String>().claim(
                Queues.INGEST,
                Queues.INGEST_GROUP_LLM,
                consumerName,
                XClaimOptions.minIdle(claimIdle).ids(message.id),
            )
            for (record in claimed) {
                val deliveryCount = message.totalDeliveryCount + 1
                if (deliveryCount > poisonMaxDeliveries) {
                    quarantine(record, "delivery count $deliveryCount > $poisonMaxDeliveries")
                    handled++
                } else if (handle(record)) {
                    handled++
                }
            }
        }
        return handled
    }

    private fun handle(record: MapRecord<String, String, String>): Boolean {
        val entry = runCatching { IngestQueueEntry.fromFields(record.value) }.getOrElse {
            quarantine(record, "schema mismatch: ${it.message}")
            return false
        }
        return runCatching {
            when (entry.type) {
                IngestType.DIGEST -> digest.process(entry)
                else -> news.process(entry)
            }
        }.fold(
            onSuccess = {
                ack(record.id)
                meters.counter("llm.processed", "type", entry.type.value).increment()
                true
            },
            onFailure = { error ->
                if (error is ClusterContendedException) {
                    log.debug("cluster contended, retry later via PEL: sourceId={}", entry.sourceId)
                    meters.counter("llm.contended").increment()
                } else {
                    log.warn("entry processing failed, left in PEL: sourceId={}", entry.sourceId, error)
                    meters.counter("llm.failed", "type", entry.type.value).increment()
                }
                false
            },
        )
    }

    fun samplePending(): Long {
        val summary = runCatching {
            redis.opsForStream<String, String>().pending(Queues.INGEST, Queues.INGEST_GROUP_LLM)
        }.getOrNull() ?: return 0
        return summary.totalPendingMessages
    }

    private fun quarantine(record: MapRecord<String, String, String>, reason: String) {
        log.warn("quarantine to DLQ: id={} reason={}", record.id, reason)
        redis.opsForStream<String, String>().add(
            StreamRecords.mapBacked<String, String, String>(record.value).withStreamKey(Queues.INGEST_DLQ),
        )
        ack(record.id)
        meters.counter("llm.dlq").increment()
    }

    private fun ack(id: RecordId) {
        redis.opsForStream<String, String>().acknowledge(Queues.INGEST, Queues.INGEST_GROUP_LLM, id)
    }
}
