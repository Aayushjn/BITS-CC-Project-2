package com.github.aayushjn.kvstore.node

import java.util.UUID

data class PhysicalNode(
    private val id: UUID = UUID.randomUUID(),
    val host: String,
    val port: Int,
) : Node, Comparable<PhysicalNode> {
    val url = "http://$host:$port"

    override fun getKey(): String = "$id-$host:$port"

    override fun hashCode(): Int = id.hashCode()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is PhysicalNode) return false
        return id == other.id
    }

    override fun compareTo(other: PhysicalNode): Int = id.compareTo(other.id)
}