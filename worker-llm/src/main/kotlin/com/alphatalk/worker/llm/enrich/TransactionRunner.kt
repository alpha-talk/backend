package com.alphatalk.worker.llm.enrich

import org.springframework.transaction.support.TransactionTemplate

fun interface TransactionRunner {
    fun run(action: () -> Unit)
}

class SpringTransactionRunner(
    private val transactions: TransactionTemplate,
) : TransactionRunner {
    override fun run(action: () -> Unit) {
        transactions.executeWithoutResult { action() }
    }
}
