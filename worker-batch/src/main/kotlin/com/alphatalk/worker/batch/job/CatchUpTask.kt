package com.alphatalk.worker.batch.job

class CatchUpTask(
    val jobName: String,
    val cron: String,
    val run: () -> Unit,
)
