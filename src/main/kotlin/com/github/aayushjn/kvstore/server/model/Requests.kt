package com.github.aayushjn.kvstore.server.model

import com.github.aayushjn.kvstore.versioning.VectorClock
import kotlinx.serialization.Serializable

@Serializable
data class GetRequest(val key: String)

@Serializable
data class DeleteRequest(val key: String)

@Serializable
data class PostRequest(val key: String, val value: String, val primaryNode: Long? = null, val clock: VectorClock? = null)
