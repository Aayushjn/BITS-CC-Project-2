package com.github.aayushjn.kvstore.node

import java.util.*

/**
 * Represents a physical node that actually runs on a host and port. Each physical node may have a number of
 * [VirtualNode]s associated with it
 */
data class PhysicalNode(
    val host: String,
    val port: Int,
) : Node {
    val url = "http://$host:$port"

    override fun getKey(): String = "$host:$port"

    override fun hashCode(): Int = Objects.hash(host, port)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is PhysicalNode) return false
        return "$host:$port" == "${other.host}:${other.port}"
    }
}