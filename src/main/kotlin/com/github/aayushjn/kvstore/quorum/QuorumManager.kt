package com.github.aayushjn.kvstore.quorum

import com.github.aayushjn.kvstore.node.PhysicalNode

class QuorumManager(replicas: Int) {
    val readQuorum : Int = if (replicas / 2 == 0) 1 else replicas / 2
    val writeQuorum = (replicas + 1) - readQuorum
    val replicaTracker = hashMapOf<String, MutableSet<PhysicalNode>>()
}
