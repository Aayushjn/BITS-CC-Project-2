package com.github.aayushjn.kvstore.versioning

data class Versioned<T>(@Volatile var data: T, val clock: VectorClock = VectorClock())

inline fun <reified T> T.versioned(clock: VectorClock = VectorClock()) = Versioned(this, clock)
