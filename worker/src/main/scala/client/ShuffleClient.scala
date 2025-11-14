package client

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.{ExecutionContext, Future, blocking, Promise}
import worker.Worker
import io.grpc.ManagedChannelBuilder
import shuffle.Shuffle.{ShuffleGrpc, DownloadRequest, DownloadResponse}
import scala.async.Async.{async, await}
import utils.PathUtils
import io.grpc.stub.StreamObserver
import java.nio.channels.FileChannel

class ShuffleClient(implicit ec: ExecutionContext) {
    val maxTries = 10

    // TODO: make it global
    val stubs = Worker.getAssignedRange.get.keys.map { case (ip, port) =>
        val channel = io.grpc.ManagedChannelBuilder.forAddress(ip, port).maxInboundMessageSize(Worker.maxGrpcMessageSize).usePlaintext().build()
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
                await { processFileWithRetry(workerIp, head) }
                await { processFilesSequentially(workerIp, tail) }
            }
        }
    }

    private def processFile(workerIp: String, filename: String, tries: Int = 1): Future[Unit] = {
        val promise = Promise[Unit]()

        val stub = Worker.synchronized(stubs(workerIp))
        val targetPath = Paths.get(s"${Worker.shuffleDir}/$filename")
        val fileChannel: FileChannel = FileChannel.open(
            targetPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        )
        
        println(s"[$workerIp, $filename] request")
        stub.downloadFile(DownloadRequest(filename = filename), new StreamObserver[DownloadResponse] {
            override def onNext(response: DownloadResponse): Unit = {
                Worker.diskIoLock.synchronized {
                    blocking { fileChannel.write(response.data.asReadOnlyByteBuffer()) }
                }
            }

            override def onError(t: Throwable): Unit = promise.failure(t)
            
            override def onCompleted(): Unit = {
                println(s"[$workerIp, $filename] response completed")
                fileChannel.close()
                promise.success(())
            }
        })

        promise.future
    }

    private def processFileWithRetry(workerIp: String, filename: String, tries: Int = 1): Future[Unit] = {
        processFile(workerIp, filename).recoverWith {
            case _ if tries < maxTries => {  // 방금 시도한게 n번째 시도이면 더이상 시도하지 않음
                println(s"Retrying [$workerIp, $filename], attempt #$tries")
                blocking { Thread.sleep(math.pow(2, tries).toLong) }
                processFileWithRetry(workerIp, filename, tries + 1)
            }
        }
    }
}