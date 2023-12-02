package com.github.aayushjn.kvstore.store.memory

import com.github.aayushjn.kvstore.store.Store
import com.github.aayushjn.kvstore.versioning.Versioned
import java.util.concurrent.ConcurrentHashMap


class InMemoryStore<K, V>(private val map: MutableMap<K, Versioned<V>> = ConcurrentHashMap()) : Store<K, V>() {
    override operator fun get(key: K): Versioned<V>? = map[key]

    override fun getAll(keys: Iterable<K>): MutableMap<K, Versioned<V>> = map.toMutableMap()

    @Synchronized
    override operator fun set(key: K, value: Versioned<V>) {
        val current = get(key)
        if (current == null) {
            map[key] = value
            return
        }

        if (current.clock < value.clock) {
            throw RuntimeException("Obsolete version for key '$key': ${value.clock}")
        }
        map[key] = value
    }

    @Synchronized
    override fun delete(key: K): Boolean {
        if (get(key) == null) {
            return false
        }
        map.remove(key)
        return true
    }
}