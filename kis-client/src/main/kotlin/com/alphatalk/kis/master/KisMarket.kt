package com.alphatalk.kis.master

enum class KisMarket(
    val fileName: String,
    val lineLength: Int,
    val listedAtOffset: Int,
    val sectorPrefix: String,
) {
    KOSPI("kospi_code", 288, 166, "0"),
    KOSDAQ("kosdaq_code", 282, 161, "1"),
}
