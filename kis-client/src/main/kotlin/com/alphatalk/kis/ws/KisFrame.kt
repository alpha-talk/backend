package com.alphatalk.kis.ws

sealed interface KisFrame {
    data class Ticks(val ticks: List<KisTick>) : KisFrame

    data class PingPong(val raw: String) : KisFrame

    data class Control(
        val trId: String?,
        val trKey: String?,
        val success: Boolean,
        val raw: String,
    ) : KisFrame

    data class EncryptedDropped(val trId: String) : KisFrame

    data class Unknown(val raw: String) : KisFrame
}
