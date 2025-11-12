package client

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.{ExecutionContext, Future, blocking}
import worker.Worker
import io.grpc.ManagedChannelBuilder
import shuffle.Shuffle.{ShuffleGrpc, DownloadRequest, DownloadResponse}
import scala.async.Async.{async, await}

class ShuffleClient(implicit ec: ExecutionContext) {
    // TODO: make it global
    val stubs = Worker.getAssignedRange.get.keys.map { case (ip, port) =>
        val channel = io.grpc.ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build()
        val stub = ShuffleGrpc.stub(channel)
        (ip, stub)
    }.toMap

    private def processFile(workerIp: String, filename: String): Future[Unit] = async {
        val stub = Worker.synchronized(stubs(workerIp))
        val bytes: DownloadResponse = await { stub.downloadFile(DownloadRequest(filename = filename)) }
        val targetPath = Paths.get(s"${Worker.shuffleDir}/$filename")
        val _  = blocking { Files.write(targetPath, bytes.data.toByteArray, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING) }
    }

}