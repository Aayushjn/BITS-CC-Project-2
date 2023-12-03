package com.github.aayushjn.kvstore.store

abstract class Store<K, V> {
    abstract operator fun get(key: K): V?
    abstract fun getAll(keys: Iterable<K>): Map<K, V>
    abstract operator fun set(key: K, value: V)
    abstract fun delete(key: K): Boolean
}