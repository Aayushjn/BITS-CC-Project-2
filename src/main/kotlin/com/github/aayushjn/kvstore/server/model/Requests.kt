package com.github.aayushjn.kvstore.server.model

import kotlinx.serialization.Serializable

@Serializable
data class GetRequest(val key: String)

@Serializable
data class DeleteRequest(val key: String)

@Serializable
data class PostRequest<V>(val key: String, val value: V)
