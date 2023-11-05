package com.github.aayushjn.kvstore.store

import com.github.aayushjn.kvstore.versioning.VectorClock
import com.github.aayushjn.kvstore.versioning.Versioned

abstract class Store<K, V> {
    abstract operator fun get(key: K): List<Versioned<V>>
    abstract fun getAll(keys: Iterable<K>): Map<K, List<Versioned<V>>>
    abstract operator fun set(key: K, value: Versioned<V>)
    abstract fun delete(key: K, clock: VectorClock?): Boolean
    abstract fun getVersions(key: K): List<VectorClock>
}