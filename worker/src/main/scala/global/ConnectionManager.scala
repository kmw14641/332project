package global

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.collection.mutable
import scala.util.Try
import scala.collection.concurrent.TrieMap

object ConnectionManager {
    val maxGrpcMessageSize: Int = 1024 * 1024 * 1024  // 1GB

    private var masterChannel: ManagedChannel = _
    private val workerChannels: TrieMap[String, ManagedChannel] = TrieMap()
    private val newWorkerBuffer: TrieMap[String, Int] = TrieMap()

    def createChannel(ip: String, port: Int): ManagedChannel = {
        ManagedChannelBuilder.forAddress(ip, port).maxInboundMessageSize(maxGrpcMessageSize).usePlaintext().build()
    }

    def initMasterChannel(ip: String, port: Int): Unit = {
        masterChannel = createChannel(ip, port)
    }
    
    def getMasterChannel(): ManagedChannel = {
        masterChannel
    }

    def initWorkerChannels(workers: Seq[(String, Int)]): Unit = this.synchronized {
        assert( workers.map(_._1).toSet.size == workers.size, "Worker IPs must be unique" )
        workers
        .map {
            case (ip, port) => (ip, newWorkerBuffer.getOrElse(ip, port))
        }
        .foreach { case (ip, port) => 
            workerChannels += ip -> createChannel(ip, port)
        }
    }

    def replaceWorkerChannel(ip: String, port: Int): Unit = this.synchronized {
        Try { getWorkerChannel(ip).shutdownNow() }
        if (workerChannels.contains(ip)) {
            workerChannels(ip) = createChannel(ip, port)
        } else {
            newWorkerBuffer += ip -> port
        }
    }

    def getWorkerChannel(ip: String): ManagedChannel = this.synchronized {
        workerChannels.getOrElse(ip, throw new NoSuchElementException(s"Worker channel for $ip not found. It should always exist."))
    }

    def shutdownAllChannels(): Unit = this.synchronized{
        masterChannel.shutdown()
        workerChannels.values.foreach(_.shutdown())
        workerChannels.clear()
    }
}