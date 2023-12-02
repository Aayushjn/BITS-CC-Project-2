package com.github.aayushjn.kvstore.serializer

import com.github.aayushjn.kvstore.versioning.VectorClock
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.mapSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

@OptIn(ExperimentalSerializationApi::class)
object VectorClockSerializer : KSerializer<VectorClock> {
    private val delegateSerializer = TreeMapSerializer
    override val descriptor = mapSerialDescriptor<Short, Long>()

    override fun deserialize(decoder: Decoder): VectorClock {
        return VectorClock(decoder.decodeSerializableValue(delegateSerializer), System.currentTimeMillis())
    }

    override fun serialize(encoder: Encoder, value: VectorClock) =
        encoder.encodeSerializableValue(delegateSerializer, value.getVersionMap())
}