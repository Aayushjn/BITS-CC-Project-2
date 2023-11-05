package com.github.aayushjn.kvstore.store.memory

import com.github.aayushjn.kvstore.store.Store
import com.github.aayushjn.kvstore.versioning.VectorClock
import com.github.aayushjn.kvstore.versioning.Versioned
import java.lang.RuntimeException
import java.util.concurrent.ConcurrentHashMap


class InMemoryStore<K, V>(private val map: MutableMap<K, MutableList<Versioned<V>>> = ConcurrentHashMap()) : Store<K, V>() {
    override operator fun get(key: K): MutableList<Versioned<V>> {
        return map[key] ?: mutableListOf()
    }

    override fun getAll(keys: Iterable<K>): MutableMap<K, MutableList<Versioned<V>>> {
        val result: MutableMap<K, MutableList<Versioned<V>>> = if (keys is Collection<*>) HashMap(keys.size) else hashMapOf()
        for (key in keys) {
            val value = get(key)
            if (value.isNotEmpty()) result[key] = value
        }
        return result
    }

    @Synchronized
    override operator fun set(key: K, value: Versioned<V>) {
        var values = map[key]
        if (values == null) {
           values = arrayListOf()
        }

        val iterator = values.iterator()
        var item: Versioned<V>
        while (iterator.hasNext()) {
            item = iterator.next()
            if (value.clock < item.clock) {
                throw RuntimeException("Obsolete version for key '$key': ${value.clock}")
            } else if (value.clock > item.clock) {
                iterator.remove()
            }
        }
        values.add(value)
        map[key] = values
    }

    @Synchronized
    override fun delete(key: K, clock: VectorClock?): Boolean {
        val values = map[key] ?: return false

        if (clock == null) {
            map.remove(key)
            return true
        }

        var deleted = false
        val iterator = values.iterator()
        var item: Versioned<V>
        while (iterator.hasNext()) {
            item = iterator.next()
            if (item.clock < clock) {
                iterator.remove()
                deleted = true
            }
        }
        if (values.isEmpty()) {
            map.remove(key)
        }
        return deleted
    }

    override fun getVersions(key: K): MutableList<VectorClock> {
        val values = get(key)
        return MutableList(values.size) { i -> values[i].clock }
    }

}