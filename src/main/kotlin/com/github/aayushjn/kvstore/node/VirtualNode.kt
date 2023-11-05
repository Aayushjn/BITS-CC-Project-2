package com.github.aayushjn.kvstore.node

class VirtualNode<T : Node>(val physicalNode: T, private val replicaIndex: Int): Node {
    override fun getKey(): String = "${physicalNode.getKey()}-$replicaIndex"

    fun isVirtualNodeOf(pNode: T): Boolean = physicalNode.getKey() == pNode.getKey()
}