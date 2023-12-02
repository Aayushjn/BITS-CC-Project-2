package com.github.aayushjn.kvstore.store

import com.github.aayushjn.kvstore.versioning.Versioned

abstract class Store<K, V> {
    abstract operator fun get(key: K): Versioned<V>?
    abstract fun getAll(keys: Iterable<K>): Map<K, Versioned<V>>
    abstract operator fun set(key: K, value: Versioned<V>)
    abstract fun delete(key: K): Boolean
}