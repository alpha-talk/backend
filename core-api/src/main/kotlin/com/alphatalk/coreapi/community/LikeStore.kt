package com.alphatalk.coreapi.community

interface LikeStore {
    fun add(postId: String, userId: Long): Boolean

    fun remove(postId: String, userId: Long): Boolean

    fun exists(postId: String, userId: Long): Boolean
}
