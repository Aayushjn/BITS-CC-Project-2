package com.github.aayushjn.kvstore.server.model

import kotlinx.serialization.Serializable

@Serializable
data class GetResponse<T>(val key: String, val value: T)

@Serializable
sealed class ErrorResponse {
    abstract val error: String

    @Serializable data class NoRoute(override val error: String = "no physical nodes to route to") : ErrorResponse()
    @Serializable data class NotFound(override val error: String = "key does not exist") : ErrorResponse()
    @Serializable data class InconsistentState(override val error: String = "data store is inconsistent") : ErrorResponse()
}
