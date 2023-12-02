package com.github.aayushjn.kvstore.serializer

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.mapSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.util.*

@OptIn(ExperimentalSerializationApi::class)
object TreeMapSerializer : KSerializer<TreeMap<Short, Long>> {
    override val descriptor = mapSerialDescriptor<Short, Long>()

    override fun deserialize(decoder: Decoder): TreeMap<Short, Long> =
        TreeMap(JsonObject.serializer().deserialize(decoder).mapKeys { it.key.toShort() }.mapValues { it.value.toString().toLong() })

    override fun serialize(encoder: Encoder, value: TreeMap<Short, Long>) =
        JsonObject.serializer().serialize(encoder, JsonObject(value.mapKeys { it.key.toString() }.mapValues { JsonPrimitive(it.value) }))
}