package com.github.aayushjn.kvstore.consistenthash

fun interface HashFunction {
    fun hash(key: String): Long
}