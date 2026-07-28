package com.alphatalk.kis.master

enum class KisMarket(
    val fileName: String,
    val lineLength: Int,
    val listedAtOffset: Int,
) {
    KOSPI("kospi_code", 288, 166),
    KOSDAQ("kosdaq_code", 282, 161),
}
