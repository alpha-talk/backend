package com.alphatalk.kis.ws

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

object KisFrameParser {
    const val TR_ID_TICK = "H0STCNT0"
    const val PINGPONG_TR_ID = "PINGPONG"

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
    private val FALLING_SIGNS = setOf("4", "5")

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
        val rtCd = json.path("body").path("rt_cd").takeIf { it.isTextual }?.asText()
        return KisFrame.Control(
            trId = trId,
            trKey = header.path("tr_key").takeIf { it.isTextual }?.asText(),
            success = rtCd == null || rtCd == "0",
            raw = text,
        )
    }

    private fun parseData(text: String): KisFrame {
        val parts = text.split("|", limit = 4)
        if (parts.size < 4) return KisFrame.Unknown(text)
        val (encrypted, trId, countText, payload) = parts
        if (encrypted == ENCRYPTED_FLAG) return KisFrame.EncryptedDropped(trId)
        if (trId != TR_ID_TICK) return KisFrame.Unknown(text)
        val count = countText.toIntOrNull()?.takeIf { it > 0 } ?: return KisFrame.Unknown(text)
        val fields = payload.split("^")
        if (fields.size % count != 0) return KisFrame.Unknown(text)
        val fieldsPerRecord = fields.size / count
        if (fieldsPerRecord < MIN_FIELDS_PER_RECORD) return KisFrame.Unknown(text)
        val ticks = (0 until count).mapNotNull { record -> toTick(fields, record * fieldsPerRecord) }
        if (ticks.size != count) return KisFrame.Unknown(text)
        return KisFrame.Ticks(ticks)
    }

    private fun toTick(fields: List<String>, base: Int): KisTick? {
        val falling = fields[base + IDX_CHANGE_SIGN] in FALLING_SIGNS
        return KisTick(
            code = fields[base + IDX_CODE],
            time = fields[base + IDX_TIME],
            price = fields[base + IDX_PRICE].toLongOrNull() ?: return null,
            change = applySign(fields[base + IDX_CHANGE].toLongOrNull() ?: return null, falling),
            changeRate = applySign(fields[base + IDX_CHANGE_RATE].toDoubleOrNull() ?: return null, falling),
            open = fields[base + IDX_OPEN].toLongOrNull() ?: return null,
            high = fields[base + IDX_HIGH].toLongOrNull() ?: return null,
            low = fields[base + IDX_LOW].toLongOrNull() ?: return null,
            volume = fields[base + IDX_ACML_VOLUME].toLongOrNull() ?: return null,
        )
    }

    private fun applySign(value: Long, falling: Boolean): Long =
        if (falling && value > 0) -value else value

    private fun applySign(value: Double, falling: Boolean): Double =
        if (falling && value > 0) -value else value
}
