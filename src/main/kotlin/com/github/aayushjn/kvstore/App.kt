package com.github.aayushjn.kvstore

import com.github.aayushjn.kvstore.node.PhysicalNode
import com.github.aayushjn.kvstore.server.Server
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.int
import java.net.InetAddress
import java.net.UnknownHostException

class App : CliktCommand() {
    private val host: String by option()
        .default("127.0.0.1")
        .help("Hostname/IP Address")
        .check("Not a valid host") {
            try {
                InetAddress.getByName(it)
                true
            } catch (e: UnknownHostException) {
                false
            }
        }
    private val port: Int by option()
        .int()
        .default(10900)
        .help("Port number")
        .check("Not a valid port number") { it in 0..65535 }
    private val pNodes: MutableList<PhysicalNode> by option()
        .convert {
            it.split(",").map { addr ->
                val split = addr.split(":")
                val isValidAddress = try {
                    InetAddress.getByName(split[0])
                    true
                } catch (e: UnknownHostException) {
                    false
                }
                val port = if (split.size == 1) 80 else split[1].toInt()
                require(isValidAddress) { "Invalid address '${split[0]}'" }
                require(port in 0..65535)
                PhysicalNode(host = split[0], port = port)
            }.toMutableList()
        }
        .default(mutableListOf())
    private val replicas: Int by option()
        .int()
        .default(1)
        .help("number of replicas")
        .check("Replicas are out of range") { it in 1..pNodes.size }

    override fun run() {
        if(PhysicalNode(host = host, port = port) !in pNodes){
            pNodes.add(PhysicalNode(host = host, port = port))
        }
        val server = Server(host, port, pNodes, replicas)
        server.start()
    }
}

fun main(args: Array<String>) = App().main(args)
