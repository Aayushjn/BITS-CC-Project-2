package com.github.aayushjn.kvstore.server

import com.github.aayushjn.kvstore.consistenthash.ConsistentHashRouter
import com.github.aayushjn.kvstore.node.PhysicalNode
import com.github.aayushjn.kvstore.quorum.QuorumManager
import com.github.aayushjn.kvstore.server.model.*
import com.github.aayushjn.kvstore.store.memory.InMemoryStore
import com.github.aayushjn.kvstore.versioning.VectorClock
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
import kotlinx.serialization.json.Json
import kotlin.math.max
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation as ClientContentNegotiation

class Server(val node: PhysicalNode, val pNodes: MutableList<PhysicalNode>, replicas : Int) {
    val store = InMemoryStore<String, String>()
    val router = ConsistentHashRouter(pNodes)
    val quorumManager = QuorumManager(replicas)
    val client = HttpClient(Java) {
        install(Logging)
        install(ClientContentNegotiation) {
            json(JSON)
        }
        install(ContentEncoding)
        defaultRequest {
            header(HttpHeaders.ContentType, "application/json")
        }
    }

    private val engine = embeddedServer(Netty, port = node.port, host = node.host) {
        install(ContentNegotiation) {
            json(JSON)
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
            getData()
            postData()
            deleteData()
        }
    }

    companion object {
        val JSON = Json {
            encodeDefaults = true
            isLenient = true
            allowSpecialFloatingPointValues = true
            allowStructuredMapKeys = true
            prettyPrint = false
            useArrayPolymorphism = false
            ignoreUnknownKeys = true
        }
    }
}

context(Server)
private fun Routing.getData() {
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
            call.respond(status = resp.status, message = resp.bodyAsText())
            return@get
        }

        if (value == null) {
            call.respond(HttpStatusCode.NotFound, ErrorResponse.NotFound())
            return@get
        }
        val quorumValues = mutableListOf<String>()
        var resp: HttpResponse
        for (n in pNodes.filter { it != node }) {
            resp = client.get(n.url) {
                parameter("isReplica", true)
                setBody(req)
            }
            if (resp.status == HttpStatusCode.OK) {
                quorumValues.add(resp.body<GetResponse>().value)
            }
            if (quorumValues.size == quorumManager.readQuorum) {
                break
            }
        }

        if (quorumValues.size != quorumManager.readQuorum) {
            call.respond(HttpStatusCode.Conflict, ErrorResponse.InconsistentState())
            return@get
        }

        if (quorumValues.all { it == value }) {
            store[req.key] = value
            call.respond(HttpStatusCode.OK, GetResponse(req.key, value))
        } else {
            call.respond(HttpStatusCode.Conflict, ErrorResponse.InconsistentState())
        }
    }
}

context(Server)
private fun Routing.postData() {
    post {
        val req = call.receive<PostRequest>()
        val isReplica = call.request.queryParameters["isReplica"]?.toBoolean() ?: false
        val pNode = router.routeNode(req.key)
        if (pNode == null) {
            call.respond(HttpStatusCode.BadRequest, ErrorResponse.NoRoute())
            return@post
        }

        val nodeId = node.getKey().hashCode().toShort()
        if (isReplica) {
            val statusCode = if (store[req.key] == null) HttpStatusCode.Created else HttpStatusCode.OK
            store[req.key] = req.value
            node.incrementClock()
            if (req.clock != null) {
                node.clock += req.clock
            }
            call.respond(statusCode, PostResponse(mapOf(nodeId to node.clock), nodeId))
            return@post
        }

        if (pNode == node) {
            val statusCode = if (store[req.key] == null) HttpStatusCode.Created else HttpStatusCode.OK
            store[req.key] = req.value
            node.incrementClock()

            val clocks = mutableMapOf<Short, VectorClock>()
            var resp: HttpResponse
            var postResp: PostResponse

            val replicaSet = quorumManager.replicaTracker[req.key] ?: pNodes
                .filter { it != node }
                .shuffled()
                .take(max(quorumManager.writeQuorum - 1, 1))

            val updatedRequest = req.copy(primaryNode = nodeId.toLong(), clock = node.clock)
            replicaSet.forEach { tempNode ->
                resp = client.post(tempNode.url) {
                    parameter("isReplica", true)
                    setBody(updatedRequest)
                }
                if (resp.status == HttpStatusCode.Created || resp.status == HttpStatusCode.OK) {
                    quorumManager.replicaTracker.compute(req.key) { _, v ->
                        v?.also { it.add(tempNode) } ?: mutableSetOf(tempNode)
                    }
                    postResp = Server.JSON.decodeFromString(resp.bodyAsText())
                    clocks[postResp.writeNode] = postResp.clocks.getOrDefault(postResp.writeNode, VectorClock())
                } else {
                    quorumManager.replicaTracker[req.key]?.forEach { n ->
                        client.delete(n.url) {
                            parameter("isReplica", true)
                            setBody(req)
                        }
                    }
                    quorumManager.replicaTracker.remove(req.key)
                    node.decrementClock()
                    call.respond(HttpStatusCode.InternalServerError, ErrorResponse.InconsistentState())
                    return@post
                }
            }
            clocks.values.forEach { node.clock += it }
            clocks[nodeId] = node.clock

            call.respond(statusCode, PostResponse(clocks, nodeId))
            return@post
        }
        val resp = client.post(pNode.url) {
            setBody(req)
        }
        call.respond(status = resp.status, message = resp.bodyAsText())
    }
}


context(Server)
private fun Routing.deleteData() {
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
                quorumManager.replicaTracker[req.key]?.forEach { n ->
                    client.delete(n.url) {
                        parameter("isReplica", true)
                        setBody(req)
                    }
                }
                quorumManager.replicaTracker.remove(req.key)
            }
            call.respond(HttpStatusCode.NoContent)
            return@delete
        }
        val resp = client.delete(pNode.url) {
            setBody(req)
        }
        call.respond(status = resp.status, message = resp.bodyAsText())
    }
}
