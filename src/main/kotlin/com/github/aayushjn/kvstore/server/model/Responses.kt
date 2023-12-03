package com.github.aayushjn.kvstore.server.model

import com.github.aayushjn.kvstore.versioning.VectorClock
import kotlinx.serialization.Serializable

@Serializable
data class GetResponse(val key: String, val value: String)

@Serializable
data class PostResponse(val clocks: Map<Short, VectorClock>, val writeNode: Short)

@Serializable
sealed class ErrorResponse {
    abstract val error: String

    @Serializable data class NoRoute(override val error: String = "no physical nodes to route to") : ErrorResponse()
    @Serializable data class NotFound(override val error: String = "key does not exist") : ErrorResponse()
    @Serializable data class InconsistentState(override val error: String = "data store is inconsistent") : ErrorResponse()
}
