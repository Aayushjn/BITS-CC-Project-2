package com.github.aayushjn.kvstore.server

import com.github.aayushjn.kvstore.consistenthash.ConsistentHashRouter
import com.github.aayushjn.kvstore.node.PhysicalNode
import com.github.aayushjn.kvstore.store.memory.InMemoryStore
import com.github.aayushjn.kvstore.versioning.Versioned
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.java.*
import io.ktor.client.plugins.compression.*
import io.ktor.client.plugins.logging.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
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
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation as ClientContentNegotiation

class Server(host: String, port: Int, private val pNodes: MutableList<PhysicalNode>, replicas : Int) {
    private val node = PhysicalNode(host = host, port = port)
    private val store = InMemoryStore<String, Any>()
    private val router = ConsistentHashRouter(pNodes)
    private val readQuorum : Int = if (replicas / 2 == 0) 1 else replicas / 2
    private val writeQuorum = (replicas + 1) - readQuorum
    private val client = HttpClient(Java) {
        install(Logging)
        install(ClientContentNegotiation) {
            json()
        }
        install(ContentEncoding)
    }
    private val replicaTracker = hashMapOf<String, MutableList<PhysicalNode>>()

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
            get {
                val data = call.receive<Map<String, Any>>()
                if ("key" !in data) {
                    call.respond(HttpStatusCode.BadRequest, mapOf("error" to "missing key"))
                    return@get
                }
                val key = data["key"].toString()
                val isReplica = call.request.queryParameters["isReplica"]?.toBoolean() ?: false

                val value = store[key]
                if (isReplica) {
                    if (value.isNotEmpty()) {
                        call.respond(HttpStatusCode.OK, mapOf(key to value.last()))
                        return@get
                    } else {
                        call.respond(HttpStatusCode.NotFound)
                    }
                }

                val pNode = router.routeNode(key)
                if (pNode == null) {
                    call.respond(HttpStatusCode.InternalServerError, mapOf("error" to "no physical nodes to route to"))
                    return@get
                }

                if (pNode != node) {
                    val resp = client.get(pNode.url) {
                        setBody(data)
                    }
                    call.respond(resp.status, resp.body())
                    return@get
                }

                val quorumValues = mutableListOf<Versioned<Any>>()
                var resp: HttpResponse
                replicaTracker[key]?.forEach { n ->
                    resp = client.get(n.url) {
                        parameter("isReplica", true)
                        setBody(mapOf("key" to key))
                    }
                    if (resp.status == HttpStatusCode.OK) {
                        // TODO: replace with Versioned only
                        quorumValues.add(resp.body())
                    }
                    if (quorumValues.all { it == value }) {
                        call.respond(HttpStatusCode.OK, mapOf(key to value))
                        return@get
                    } else {
                        call.respond(HttpStatusCode.InternalServerError)
                    }
                }
            }

            post {
                val data = call.receive<Map<String, Any>>()
                val key = data.keys.first()
                val isReplica = call.request.queryParameters["isReplica"]?.toBoolean() ?: false
                val pNode = router.routeNode(key)
                if (pNode == null) {
                    call.respond(HttpStatusCode.InternalServerError, mapOf("error" to "no physical nodes to route to"))
                    return@post
                }
                if (pNode == node || isReplica) {
                    val value = store[key].last()
                    value.data = data.getValue(key)
                    value.clock.increment(node.getKey().hashCode().toUShort(), System.currentTimeMillis())
                    store[key] = value
                    if (!isReplica) {
                        var resp: HttpResponse
                        pNodes.filter { it != node }.shuffled().take(writeQuorum - 1).forEach { tempNode ->
                            resp = client.post(tempNode.url) {
                                parameter("isReplica", true)
                                setBody(data)
                            }
                            if (resp.status == HttpStatusCode.Created) {
                                replicaTracker.compute(key) { _, v ->
                                    v?.also { it.add(tempNode) } ?: mutableListOf(tempNode)
                                }
                            } else {
                                replicaTracker[key]?.forEach { n ->
                                    client.delete(n.url) {
                                        parameter("isReplica", true)
                                        setBody(mapOf("key" to key))
                                    }
                                }
                                replicaTracker.remove(key)
                                call.respond(HttpStatusCode.InternalServerError)
                                return@post
                            }
                        }
                    }
                    call.respond(HttpStatusCode.Created)
                    return@post
                }
                val resp = client.post(pNode.url) {
                    setBody(data)
                }
                call.respond(resp.status, resp.body())
            }

            delete {
                val data = call.receive<Map<String, Any>>()
                val isReplica = call.request.queryParameters["isReplica"]?.toBoolean() ?: false
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
                if (pNode == node || isReplica) {
                    store.delete(key, null)

                    if (!isReplica) {
                        replicaTracker[key]?.forEach { n ->
                            client.delete(n.url) {
                                parameter("isReplica", true)
                                setBody(mapOf("key" to key))
                            }
                        }
                        replicaTracker.remove(key)
                        call.respond(HttpStatusCode.InternalServerError)
                        return@delete
                    }
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
