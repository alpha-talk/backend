package com.alphatalk.kis

internal object KisSigns {
    private val FALLING = setOf("4", "5")

    fun isFalling(sign: String): Boolean = sign in FALLING

    fun apply(value: Long, falling: Boolean): Long = if (falling && value > 0) -value else value

    fun apply(value: Double, falling: Boolean): Double = if (falling && value > 0) -value else value
}
