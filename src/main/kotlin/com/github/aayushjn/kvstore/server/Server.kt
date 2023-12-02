package com.github.aayushjn.kvstore.server

import com.github.aayushjn.kvstore.consistenthash.ConsistentHashRouter
import com.github.aayushjn.kvstore.node.PhysicalNode
import com.github.aayushjn.kvstore.server.model.*
import com.github.aayushjn.kvstore.store.memory.InMemoryStore
import com.github.aayushjn.kvstore.versioning.Versioned
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.java.*
import io.ktor.client.plugins.*
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
import io.ktor.server.plugins.defaultheaders.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.util.reflect.*
import kotlin.math.max
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation as ClientContentNegotiation

class Server(pNode: PhysicalNode, private val pNodes: MutableList<PhysicalNode>, replicas : Int) {
    private val node = pNode
    private val store = InMemoryStore<String, String>()
    private val router = ConsistentHashRouter(pNodes)
    private val readQuorum : Int = if (replicas / 2 == 0) 1 else replicas / 2
    private val writeQuorum = (replicas + 1) - readQuorum
    private val client = HttpClient(Java) {
        install(Logging)
        install(ClientContentNegotiation) {
            json()
        }
        install(ContentEncoding)
        defaultRequest {
            header(HttpHeaders.ContentType, "application/json")
        }
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
        install(DefaultHeaders) {
            header(HttpHeaders.ContentType, "application/json")
        }
        configureRouting()
    }

    fun start() {
        engine.start(wait = true)
    }

    fun stop() = engine.stop()

    private fun Application.configureRouting() {
        routing {
            get {
                val req = call.receive<GetRequest>()
                val isReplica = call.request.queryParameters["isReplica"]?.toBoolean() ?: false

                val value = store[req.key]
                if (isReplica) {
                    if (value != null) {
                        call.respond(HttpStatusCode.OK, GetResponse(req.key, value))
                    } else {
                        call.respond(HttpStatusCode.NotFound, ErrorResponse.NotFound())
                    }
                    return@get
                }

                val pNode = router.routeNode(req.key)
                if (pNode == null) {
                    call.respond(HttpStatusCode.BadRequest, ErrorResponse.NoRoute())
                    return@get
                }

                if (pNode != node) {
                    val resp = client.get(pNode.url) {
                        setBody(req)
                    }
                    call.respond(resp.status, resp.bodyAsText())
                    return@get
                }

                if (value == null) {
                    call.respond(HttpStatusCode.NotFound, ErrorResponse.NotFound())
                    return@get
                }
                val quorumValues = mutableListOf<Versioned<String>>()
                var resp: HttpResponse
                for (n in pNodes.filter { it != node }) {
                    resp = client.get(n.url) {
                        parameter("isReplica", true)
                        setBody(req)
                    }
                    if (resp.status == HttpStatusCode.OK) {
                        quorumValues.add(resp.body<GetResponse<Versioned<String>>>().value)
                    }
                    if (quorumValues.size == readQuorum) {
                        break
                    }
                }

                if (quorumValues.all { it.data == value.data }) {
                    quorumValues.forEach { v -> value.clock.merge(v.clock) }
                    store[req.key] = value
                    call.respond(HttpStatusCode.OK, GetResponse(req.key, value))
                } else {
                    call.respond(HttpStatusCode.Conflict, ErrorResponse.InconsistentState())
                }
            }

            post {
                val req = call.receive<PostRequest<String>>()
                val isReplica = call.request.queryParameters["isReplica"]?.toBoolean() ?: false
                val pNode = router.routeNode(req.key)
                if (pNode == null) {
                    call.respond(HttpStatusCode.BadRequest, ErrorResponse.NoRoute())
                    return@post
                }
                if (pNode == node || isReplica) {
                    var statusCode = HttpStatusCode.Created
                    store[req.key] = store[req.key]?.also {
                        it.data = req.value
                        it.clock.inc(node.getKey().hashCode().toUShort())
                        statusCode = HttpStatusCode.NoContent
                    } ?: Versioned(req.value).also { it.clock.inc(node.getKey().hashCode().toUShort()) }

                    if (!isReplica) {
                        var resp: HttpResponse
                        println(pNodes)
                        pNodes.filter { it != node }.shuffled().take(max(writeQuorum - 1, 1)).forEach { tempNode ->
                            resp = client.post(tempNode.url) {
                                parameter("isReplica", true)
                                setBody(req)
                            }
                            if (resp.status == HttpStatusCode.Created || resp.status == HttpStatusCode.NoContent) {
                                replicaTracker.compute(req.key) { _, v ->
                                    v?.also { it.add(tempNode) } ?: mutableListOf(tempNode)
                                }
                            } else {
                                replicaTracker[req.key]?.forEach { n ->
                                    client.delete(n.url) {
                                        parameter("isReplica", true)
                                        setBody(req)
                                    }
                                }
                                replicaTracker.remove(req.key)
                                call.respond(HttpStatusCode.InternalServerError, ErrorResponse.InconsistentState())
                                return@post
                            }
                        }
                    }
                    call.respond(statusCode)
                    return@post
                }
                val resp = client.post(pNode.url) {
                    setBody(req)
                }
                call.respond(resp.status, resp.bodyAsText())
            }

            delete {
                val req = call.receive<DeleteRequest>()
                val isReplica = call.request.queryParameters["isReplica"]?.toBoolean() ?: false
                val pNode = router.routeNode(req.key)
                if (pNode == null) {
                    call.respond(HttpStatusCode.BadRequest, ErrorResponse.NoRoute())
                    return@delete
                }
                if (pNode == node || isReplica) {
                    if (store[req.key] == null) {
                        call.respond(HttpStatusCode.NotFound, ErrorResponse.NotFound())
                        return@delete
                    }
                    store.delete(req.key)

                    if (!isReplica) {
                        replicaTracker[req.key]?.forEach { n ->
                            client.delete(n.url) {
                                parameter("isReplica", true)
                                setBody(req)
                            }
                        }
                        replicaTracker.remove(req.key)
                    }
                    call.respond(HttpStatusCode.NoContent)
                    return@delete
                }
                val resp = client.delete(pNode.url) {
                    setBody(req)
                }
                call.respond(resp.status, resp.bodyAsText())
            }
        }
    }
}
