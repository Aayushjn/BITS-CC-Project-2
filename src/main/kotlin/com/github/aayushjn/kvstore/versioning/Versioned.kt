package com.github.aayushjn.kvstore.versioning

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * [Versioned] wraps arbitrary data with a vector clock to maintain version information for storage
 *
 * @param T type of data
 */
@Serializable
data class Versioned<T>(@Volatile var data: T, @SerialName("vectorClock") val clock: VectorClock = VectorClock())
