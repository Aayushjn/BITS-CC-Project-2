package com.github.aayushjn.kvstore.node

/**
 * Represents a virtual node that maps to a portion of the consistent hash ring
 */
class VirtualNode<T : Node>(val physicalNode: T, private val replicaIndex: Int): Node {
    override fun getKey(): String = "${physicalNode.getKey()}-$replicaIndex"

    fun isVirtualNodeOf(pNode: T): Boolean = physicalNode.getKey() == pNode.getKey()
}