package com.alphatalk.worker.price.config

import com.alphatalk.kis.model.KisAccount
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

class KisAccounts(val values: List<KisAccount>) {
    init {
        check(values.isNotEmpty()) { "KIS 수집에는 KIS_ACCOUNTS 계정이 최소 1개 필요하다" }
    }

    companion object {
        fun parse(accountsJson: String): KisAccounts {
            val accounts: List<KisAccount> = try {
                jacksonObjectMapper().readValue(accountsJson)
            } catch (e: Exception) {
                throw IllegalStateException(
                    "KIS_ACCOUNTS JSON 파싱 실패 — [{\"keyId\",\"appkey\",\"appsecret\"}] 배열이어야 한다",
                )
            }
            return KisAccounts(accounts)
        }
    }
}
