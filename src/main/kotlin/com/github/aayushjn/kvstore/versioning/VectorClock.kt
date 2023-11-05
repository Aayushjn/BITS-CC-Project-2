package com.github.aayushjn.kvstore.versioning

import java.util.TreeMap
import java.util.TreeSet
import kotlin.math.max

data class VectorClock(
    private var versionMap: TreeMap<Short, Long> = TreeMap(),
    @Volatile private var timestamp: Long
) : Comparable<VectorClock> {
    constructor() : this(timestamp = System.currentTimeMillis())

    fun increment(node: UShort, time: Long) {
        timestamp = time
        versionMap.compute(node.toShort()) { _, v -> if (v == null) 1 else v + 1 }
    }

    fun merge(clock: VectorClock): VectorClock {
        val newClock = VectorClock(versionMap, System.currentTimeMillis())
        var version: Long?
        for (entry in clock.versionMap.entries) {
            version = newClock.versionMap[entry.key]
            newClock.versionMap[entry.key] = (if (version == null) entry.value else max(entry.value, version))
        }
        return newClock
    }

    override fun compareTo(other: VectorClock): Int {
        var isGreater = false
        var isSmaller = false

        val selfNodes = versionMap.navigableKeySet()
        val otherNodes = other.versionMap.navigableKeySet()

        val commonNodes = TreeSet(selfNodes)
        commonNodes.retainAll(otherNodes)

        if (selfNodes.size > commonNodes.size) isGreater = true
        if (otherNodes.size > commonNodes.size) isSmaller = true

        var selfValue: Long
        var otherValue: Long
        for (nodeId in commonNodes) {
            if (isGreater && isSmaller) break

            selfValue = versionMap.getValue(nodeId)
            otherValue = other.versionMap.getValue(nodeId)
            if (selfValue > otherValue) {
                isGreater = true
            } else if (otherValue > selfValue) {
                isSmaller = true
            }
        }

        return if (!isGreater && isSmaller) -1 else if (isGreater && !isSmaller) 1 else 0
    }
}