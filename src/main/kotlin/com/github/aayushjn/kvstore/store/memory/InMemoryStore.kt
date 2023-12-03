package com.github.aayushjn.kvstore.store.memory

import com.github.aayushjn.kvstore.store.Store
import java.util.concurrent.ConcurrentHashMap


/**
 * An implementation of [Store] that maintains data in-memory. Once the application terminates, all data is lost.
 */
class InMemoryStore<K, V>(private val map: MutableMap<K, V> = ConcurrentHashMap()) : Store<K, V>() {
    override operator fun get(key: K): V? = map[key]

    override fun getAll(keys: Iterable<K>): MutableMap<K, V> = map.toMutableMap()

    @Synchronized
    override operator fun set(key: K, value: V) {
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