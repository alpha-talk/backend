package com.alphatalk.kis.ws

interface KisSessionListener {
    fun onOpen() {}
    fun onTicks(ticks: List<KisTick>) {}
    fun onSubscribeAck(trId: String?, trKey: String?, success: Boolean) {}
    fun onPingPong() {}
    fun onEncryptedDropped(trId: String) {}
    fun onClosed(reason: String?) {}
    fun onError(t: Throwable) {}
    fun onTransportError(t: Throwable) {
        onError(t)
    }
}
