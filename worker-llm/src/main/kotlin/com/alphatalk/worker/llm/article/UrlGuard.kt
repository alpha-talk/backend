package com.alphatalk.worker.llm.article

import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress
import java.net.URI

object UrlGuard {
    fun safeUrl(url: String): URI? {
        val uri = runCatching { URI(url.trim()) }.getOrNull() ?: return null
        val scheme = uri.scheme?.lowercase()
        if (scheme != "http" && scheme != "https") return null
        val host = uri.host ?: return null
        val addresses = runCatching { InetAddress.getAllByName(host) }.getOrNull() ?: return null
        if (addresses.isEmpty() || addresses.any { !isPublic(it) }) return null
        return uri
    }

    private fun isPublic(address: InetAddress): Boolean = !(
        address.isLoopbackAddress ||
            address.isAnyLocalAddress ||
            address.isLinkLocalAddress ||
            address.isSiteLocalAddress ||
            address.isMulticastAddress ||
            isCarrierGradeNat(address) ||
            isUniqueLocalIpv6(address)
        )

    private fun isCarrierGradeNat(address: InetAddress): Boolean {
        if (address !is Inet4Address) return false
        val bytes = address.address
        return (bytes[0].toInt() and 0xff) == 100 && (bytes[1].toInt() and 0xc0) == 64
    }

    private fun isUniqueLocalIpv6(address: InetAddress): Boolean {
        if (address !is Inet6Address) return false
        return (address.address[0].toInt() and 0xfe) == 0xfc
    }
}
