package com.alphatalk.kis.ws

import com.alphatalk.kis.KisSigns
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

object KisFrameParser {
    const val TR_ID_TICK = "H0STCNT0"
    const val TR_ID_TICK_TOTAL = "H0UNCNT0"
    const val TR_ID_TICK_OVERTIME = "H0STOUP0"
    val TICK_TR_IDS = setOf(TR_ID_TICK, TR_ID_TICK_TOTAL, TR_ID_TICK_OVERTIME)
    const val TR_ID_DEPTH = "H0STASP0"
    const val TR_ID_DEPTH_TOTAL = "H0UNASP0"
    val DEPTH_TR_IDS = setOf(TR_ID_DEPTH, TR_ID_DEPTH_TOTAL)
    const val PINGPONG_TR_ID = "PINGPONG"
    private const val UNSUBSCRIBE_MESSAGE_PREFIX = "UNSUB"

    private const val ENCRYPTED_FLAG = "1"
    private const val IDX_CODE = 0
    private const val IDX_TIME = 1
    private const val IDX_PRICE = 2
    private const val IDX_CHANGE_SIGN = 3
    private const val IDX_CHANGE = 4
    private const val IDX_CHANGE_RATE = 5
    private const val IDX_OPEN = 7
    private const val IDX_HIGH = 8
    private const val IDX_LOW = 9
    private const val IDX_ACML_VOLUME = 13
    private const val MIN_FIELDS_PER_RECORD = 14
    private const val DEPTH_LEVELS = 10
    private const val DEPTH_IDX_ASK_PRICE = 3
    private const val DEPTH_IDX_BID_PRICE = 13
    private const val DEPTH_IDX_ASK_QTY = 23
    private const val DEPTH_IDX_BID_QTY = 33
    private const val MIN_FIELDS_PER_DEPTH_RECORD = 43

    private val mapper: ObjectMapper = jacksonObjectMapper()

    fun parse(text: String): KisFrame {
        val trimmed = text.trim()
        return if (trimmed.startsWith("{")) parseControl(trimmed) else parseData(trimmed)
    }

    private fun parseControl(text: String): KisFrame {
        val json = runCatching { mapper.readTree(text) }.getOrNull() ?: return KisFrame.Unknown(text)
        val header = json.path("header")
        val trId = header.path("tr_id").takeIf { it.isTextual }?.asText()
        if (trId == PINGPONG_TR_ID) return KisFrame.PingPong(text)
        val body = json.path("body")
        val rtCd = body.path("rt_cd").takeIf { it.isTextual }?.asText()
        val message = body.path("msg1").takeIf { it.isTextual }?.asText().orEmpty().trim().uppercase()
        return KisFrame.Control(
            trId = trId,
            trKey = header.path("tr_key").takeIf { it.isTextual }?.asText(),
            success = rtCd == null || rtCd == "0",
            unsubscribe = message.startsWith(UNSUBSCRIBE_MESSAGE_PREFIX),
            raw = text,
        )
    }

    private fun parseData(text: String): KisFrame {
        val parts = text.split("|", limit = 4)
        if (parts.size < 4) return KisFrame.Unknown(text)
        val (encrypted, trId, countText, payload) = parts
        if (encrypted == ENCRYPTED_FLAG) return KisFrame.EncryptedDropped(trId)
        if (trId !in TICK_TR_IDS && trId !in DEPTH_TR_IDS) return KisFrame.Unknown(text)
        val count = countText.toIntOrNull()?.takeIf { it > 0 } ?: return KisFrame.Unknown(text)
        val fields = payload.split("^")
        if (fields.size % count != 0) return KisFrame.Unknown(text)
        val fieldsPerRecord = fields.size / count
        if (trId in DEPTH_TR_IDS) {
            if (fieldsPerRecord < MIN_FIELDS_PER_DEPTH_RECORD) return KisFrame.Unknown(text)
            val depths = (0 until count).mapNotNull { record -> toDepth(fields, record * fieldsPerRecord) }
            if (depths.size != count) return KisFrame.Unknown(text)
            return KisFrame.Depths(trId, depths)
        }
        if (fieldsPerRecord < MIN_FIELDS_PER_RECORD) return KisFrame.Unknown(text)
        val ticks = (0 until count).mapNotNull { record -> toTick(fields, record * fieldsPerRecord) }
        if (ticks.size != count) return KisFrame.Unknown(text)
        return KisFrame.Ticks(trId, ticks)
    }

    private fun toDepth(fields: List<String>, base: Int): KisDepth? {
        val asks = depthLevels(fields, base + DEPTH_IDX_ASK_PRICE, base + DEPTH_IDX_ASK_QTY) ?: return null
        val bids = depthLevels(fields, base + DEPTH_IDX_BID_PRICE, base + DEPTH_IDX_BID_QTY) ?: return null
        return KisDepth(
            code = fields[base + IDX_CODE],
            time = fields[base + IDX_TIME],
            asks = asks,
            bids = bids,
        )
    }

    private fun depthLevels(fields: List<String>, priceBase: Int, qtyBase: Int): List<KisDepthLevel>? =
        (0 until DEPTH_LEVELS).map { level ->
            KisDepthLevel(
                price = fields[priceBase + level].toLongOrNull() ?: return null,
                qty = fields[qtyBase + level].toLongOrNull() ?: return null,
            )
        }

    private fun toTick(fields: List<String>, base: Int): KisTick? {
        val falling = KisSigns.isFalling(fields[base + IDX_CHANGE_SIGN])
        return KisTick(
            code = fields[base + IDX_CODE],
            time = fields[base + IDX_TIME],
            price = fields[base + IDX_PRICE].toLongOrNull() ?: return null,
            change = KisSigns.apply(fields[base + IDX_CHANGE].toLongOrNull() ?: return null, falling),
            changeRate = KisSigns.apply(fields[base + IDX_CHANGE_RATE].toDoubleOrNull() ?: return null, falling),
            open = fields[base + IDX_OPEN].toLongOrNull() ?: return null,
            high = fields[base + IDX_HIGH].toLongOrNull() ?: return null,
            low = fields[base + IDX_LOW].toLongOrNull() ?: return null,
            volume = fields[base + IDX_ACML_VOLUME].toLongOrNull() ?: return null,
        )
    }
}
