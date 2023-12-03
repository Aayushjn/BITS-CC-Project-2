package com.github.aayushjn.kvstore.versioning

import com.github.aayushjn.kvstore.serializer.TreeMapSerializer
import com.github.aayushjn.kvstore.serializer.VectorClockSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.util.TreeMap
import java.util.TreeSet
import kotlin.math.max

/**
 * Represents a vector clock represented as a [TreeMap]. [versionMap] maintains the mapping of each node's vector
 * timestamp, while [timestamp] is used to maintain wall-clock timing of updates
 */
@Serializable(with = VectorClockSerializer::class)
data class VectorClock(
    @Serializable(with = TreeMapSerializer::class) @SerialName("vectorClock") private var versionMap: TreeMap<Short, Long> = TreeMap(),
    @Volatile private var timestamp: Long
) : Comparable<VectorClock> {
    constructor() : this(timestamp = System.currentTimeMillis())

    fun getVersionMap(): TreeMap<Short, Long> = versionMap

    operator fun inc(node: UShort): VectorClock = inc(node, System.currentTimeMillis())

    operator fun dec(node: UShort): VectorClock = dec(node, System.currentTimeMillis())

    operator fun inc(node: UShort, time: Long): VectorClock {
        timestamp = time
        versionMap.compute(node.toShort()) { _, v -> if (v == null) 1 else v + 1 }
        return this
    }

    operator fun dec(node: UShort, time: Long): VectorClock {
        timestamp = time
        versionMap.compute(node.toShort()) { _, v -> if (v == null) 0 else max(0, v - 1) }
        return this
    }

    operator fun plus(clock: VectorClock): VectorClock {
        val newClock = copy(versionMap = versionMap, timestamp = System.currentTimeMillis())
        for (entry in clock.versionMap.entries) {
            newClock.versionMap.compute(entry.key) { _, v -> if (v == null) entry.value else max(entry.value, v) }
        }
        return newClock
    }

    operator fun plusAssign(clock: VectorClock) {
        val newClock = plus(clock)
        versionMap = newClock.versionMap
        timestamp = newClock.timestamp
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