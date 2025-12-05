package global

import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import scala.collection.mutable

object ConnectionManager {
    val maxGrpcMessageSize: Int = 1024 * 1024 * 1024  // 1GB

    private var masterChannel: ManagedChannel = _
    private var workerChannels: mutable.Map[String, ManagedChannel] = mutable.Map()

    def createChannel(ip: String, port: Int): ManagedChannel = {
        ManagedChannelBuilder.forAddress(ip, port).maxInboundMessageSize(maxGrpcMessageSize).usePlaintext().build()
    }

    def initMasterChannel(ip: String, port: Int): Unit = {
        masterChannel = createChannel(ip, port)
    }
    
    def getMasterChannel(): ManagedChannel = {
        masterChannel
    }

    // prioritize replace request than init request:
    // if there is multiple re-register from one worker,
    // we have to consider later request arrives first, and overwritten by no longer valid request
    // (it may be resolved by timestamp in request)
    // however, we have constraint that only one fault occurs,
    // so just considering replace request as newer request resolves this problem easily
    def initWorkerChannels(workers: Seq[(String, Int)]): Unit = this.synchronized {
        assert( workers.map(_._1).toSet.size == workers.size, "Worker IPs must be unique" )
        workers.foreach { case (ip, port) => 
            if (!workerChannels.contains(ip)) {
                workerChannels(ip) = createChannel(ip, port)
            }
        }
    }

    def setWorkerChannel(ip: String, port: Int): Unit = this.synchronized {
        workerChannels.get(ip).foreach(ch => {
            ch.shutdown()
            println(s"Shutdown invalid channel of $ip")
        })
        workerChannels(ip) = createChannel(ip, port)
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