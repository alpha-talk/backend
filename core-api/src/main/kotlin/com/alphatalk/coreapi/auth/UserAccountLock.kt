package com.alphatalk.coreapi.auth

interface UserAccountLock {
    fun acquire(userId: Long): Boolean
}
