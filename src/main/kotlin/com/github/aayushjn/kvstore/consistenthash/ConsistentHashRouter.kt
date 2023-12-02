package com.github.aayushjn.kvstore.consistenthash

import com.github.aayushjn.kvstore.node.Node
import com.github.aayushjn.kvstore.node.VirtualNode
import com.google.common.hash.Hashing
import java.nio.charset.Charset
import java.util.*


/**
 * Handles routing duties for consistent hashing across dynamically changing nodes
 */
class ConsistentHashRouter<T : Node>(
    physicalNodes: Collection<T>,
    virtualNodeCount: Int = DEFAULT_VNODE_COUNT,
    private val hashFunction: HashFunction = DefaultHashFunction,
) {
    private val ring: TreeMap<Long, VirtualNode<T>> = TreeMap()

    init {
        physicalNodes.forEach { pNode -> addNode(pNode, virtualNodeCount) }
    }

    fun addNode(pNode: T, virtualNodeCount: Int) {
        val existingReplicas = getExistingReplicas(pNode)
        var vNode: VirtualNode<T>
        for (i in 0..<virtualNodeCount) {
            vNode = VirtualNode(pNode, existingReplicas + i)
            ring[hashFunction.hash(vNode.getKey())] = vNode
        }
    }

    fun removeNode(pNode: T) {
        var vNode: VirtualNode<T>
        for (key in ring.keys) {
            vNode = ring.getValue(key)
            if (vNode.isVirtualNodeOf(pNode)) ring.remove(key)
        }
    }

    fun routeNode(key: String): T? {
        if (ring.isEmpty()) return null

        val hash = hashFunction.hash(key)
        val tailMap = ring.tailMap(hash)
        val nodeHash = if (tailMap.isNotEmpty()) tailMap.firstKey() else ring.firstKey()
        return ring.getValue(nodeHash).physicalNode
    }

    fun getExistingReplicas(pNode: T): Int {
        var replicas = 0
        for (vNode in ring.values) {
            if (vNode.isVirtualNodeOf(pNode)) replicas++
        }
        return replicas
    }

    companion object {
        const val DEFAULT_VNODE_COUNT = 500

        /**
         * Default hash function implementation that relies on SipHash for a 2^64 id-space
         */
        object DefaultHashFunction: HashFunction {
            override fun hash(key: String): Long = Hashing.sipHash24().hashString(
                key,
                Charset.defaultCharset()
            ).padToLong()
        }
    }
}
