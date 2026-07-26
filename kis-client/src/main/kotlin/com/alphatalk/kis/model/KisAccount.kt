package com.alphatalk.kis.model

data class KisAccount(
    val keyId: String,
    val appkey: String,
    val appsecret: String,
) {
    override fun toString() = "KisAccount(keyId=$keyId)"
}
