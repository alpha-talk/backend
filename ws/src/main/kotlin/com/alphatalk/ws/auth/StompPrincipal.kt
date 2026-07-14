package com.alphatalk.ws.auth

import java.security.Principal

data class StompPrincipal(val userId: Long) : Principal {
    override fun getName(): String = userId.toString()
}
