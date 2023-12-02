package com.github.aayushjn.kvstore.versioning

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
data class Versioned<T>(@Volatile var data: T, @SerialName("vectorClock") val clock: VectorClock = VectorClock())
