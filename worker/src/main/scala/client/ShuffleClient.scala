package client

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.{ExecutionContext, Future, blocking}
import worker.Worker
import io.grpc.ManagedChannelBuilder
import shuffle.Shuffle.{ShuffleGrpc, DownloadRequest, DownloadResponse}
import scala.async.Async.{async, await}
import utils.PathUtils

class ShuffleClient(implicit ec: ExecutionContext) {
    // TODO: make it global
    val stubs = Worker.getAssignedRange.get.keys.map { case (ip, port) =>
        val channel = io.grpc.ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build()
        val stub = ShuffleGrpc.stub(channel)
        (ip, stub)
    }.toMap
    
	def start(receiverFileInfo: Map[String, List[String]]): Future[Unit] = async {  // TODO: make input as optional, if none, restore
        PathUtils.createDirectoryIfNotExists(Worker.shuffleDir)
        val workerFutures = receiverFileInfo.map {
            case (workerIp, fileList) => processFilesSequentially(workerIp, fileList)
        }
        await { Future.sequence(workerFutures) }
    }

    private def processFilesSequentially(workerIp: String, fileList: List[String]): Future[Unit] = async {
        fileList match {
            case Nil => Future.successful()
            case head :: tail => {
                await { processFile(workerIp, head) }
                await { processFilesSequentially(workerIp, tail) }
            }
        }
    }

    private def processFile(workerIp: String, filename: String): Future[Unit] = async {
        println(s"[$workerIp, $filename] SEND")
        val stub = Worker.synchronized(stubs(workerIp))
        val bytes: DownloadResponse = await { stub.downloadFile(DownloadRequest(filename = filename)) }
        val targetPath = Paths.get(s"${Worker.shuffleDir}/$filename")
        val _ = blocking { Files.write(targetPath, bytes.data.toByteArray, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING) }
        println(s"[$workerIp, $filename] RECEIVE")
    }

}