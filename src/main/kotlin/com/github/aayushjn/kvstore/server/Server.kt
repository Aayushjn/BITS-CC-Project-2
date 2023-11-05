package com.github.aayushjn.kvstore.server

import com.github.aayushjn.kvstore.consistenthash.ConsistentHashRouter
import com.github.aayushjn.kvstore.node.PhysicalNode
import com.github.aayushjn.kvstore.store.memory.InMemoryStore
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.java.*
import io.ktor.client.plugins.compression.*
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation as ClientContentNegotiation
import io.ktor.client.plugins.logging.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.callid.*
import io.ktor.server.plugins.callloging.*
import io.ktor.server.plugins.compression.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

class Server(host: String, port: Int, pNodes: List<PhysicalNode>) {
    private val node = PhysicalNode(host = host, port = port)
    private val store = InMemoryStore<String, Any>()
    private val router = ConsistentHashRouter(pNodes)
    private val client = HttpClient(Java) {
        install(Logging)
        install(ClientContentNegotiation) {
            json()
        }
        install(ContentEncoding)
    }

    private val engine = embeddedServer(Netty, port = node.port, host = node.host) {
        install(ContentNegotiation) {
            json()
        }
        install(Compression) {
            default()
        }
        install(CallLogging)
        install(CallId)
        configureRouting()
    }

    fun start() {
        engine.start(wait = true)
    }

    fun stop() = engine.stop()

    private fun Application.configureRouting() {
        routing {
            post {
                val data = call.receive<Map<String, Any>>()
                val key = data.keys.first()
                val pNode = router.routeNode(key)
                if (pNode == null) {
                    call.respond(HttpStatusCode.InternalServerError, mapOf("error" to "no physical nodes to route to"))
                    return@post
                }
                if (pNode == node) {
                    val value = store[key].last()
                    value.data = data.getValue(key)
                    value.clock.increment(node.getKey().hashCode().toUShort(), System.currentTimeMillis())
                    store[key] = value
                    // TODO: Write to replica physical nodes as per quorum
                    // TODO: Return parameters in below line
                    call.respond(HttpStatusCode.OK)
                    return@post
                }
                val resp = client.post(pNode.url) {
                    setBody(data)
                }
                call.respond(resp.status, resp.body())
            }

            delete {
                val data = call.receive<Map<String, Any>>()
                if ("key" !in data) {
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "missing key"))
                    return@delete
                }
                val key = data["key"].toString()
                val pNode = router.routeNode(key)
                if (pNode == null) {
                    call.respond(HttpStatusCode.InternalServerError, mapOf("error" to "no physical nodes to route to"))
                    return@delete
                }
                if (pNode == node) {
                    store.delete(key, null)
                    // TODO: Delete from replicas as per quorum
                    call.respond(HttpStatusCode.OK)
                    return@delete
                }
                val resp = client.delete(pNode.url) {
                    setBody(data)
                }
                call.respond(resp.status, resp.body())
            }
        }
    }
}
