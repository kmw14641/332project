package main

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.{ExecutionContext, Future, blocking, Promise}
import global.WorkerState
import io.grpc.ManagedChannelBuilder
import shuffle.Shuffle.{ShuffleGrpc, DownloadRequest, DownloadResponse}
import scala.async.Async.{async, await}
import global.ConnectionManager
import utils.FileManager
import io.grpc.stub.{StreamObserver, ClientCallStreamObserver, ClientResponseObserver}
import java.nio.channels.FileChannel
import common.utils.SystemUtils

class ShuffleManager(subDirName: String)(implicit ec: ExecutionContext) {
    val maxTries = 10

	def start(shufflePlans: Map[String, Seq[String]]): Future[List[String]] = async {  // TODO: make input as optional, if none, restore
        val selfIp = SystemUtils.getLocalIp.get

        val workerFutures = shufflePlans.map {
            case (workerIp, fileList) if workerIp != selfIp => processFilesSequentially(workerIp, fileList)
            case (workerIp, fileList) => Future.successful(moveLocalFiles(fileList))
        }
        await { Future.sequence(workerFutures) }
        shufflePlans.values.flatten.toList
    }

    private def moveLocalFiles(fileList: Seq[String]): Unit = {
        fileList.foreach { filename =>
            println(s"[Local Shuffle] Moving file: $filename")
            FileManager.move(filename, filename, subDirName)
        }
    }

    private def processFilesSequentially(workerIp: String, fileList: Seq[String]): Future[Unit] = async {
        fileList match {
            case Nil => ()
            case head :: tail => {
                println(s"Processing file [$workerIp, $head]")
                await { processFileWithRetry(workerIp, head) }
                await { processFilesSequentially(workerIp, tail) }
            }
        }
    }

    private def processFile(workerIp: String, filename: String, tries: Int = 1): Future[Unit] = {
        val promise = Promise[Unit]()

        val stub = ShuffleGrpc.stub(ConnectionManager.getWorkerChannel(workerIp))
        val targetFilename = FileManager.createAndRegister(subDirName, Some(filename))
        val targetPath = FileManager.getPhysicalFilePath(targetFilename)
        val fileChannel: FileChannel = FileChannel.open(
            Paths.get(targetPath),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        )

        def tryClose(fileChannel: FileChannel): Unit = {
            try { fileChannel.close() } catch {
                // 원래 예외를 보존하기 위해 catch 블록의 에러는 바로 처리 (로그만 남김)
                case e: Throwable => println(s"[WARN] Failed to close fileChannel for [$workerIp, $filename]: ${e.getMessage}")
            }
        }
        
        println(s"[$workerIp, $filename] request")
        val observer = new ClientResponseObserver[DownloadRequest, DownloadResponse] {
            // ClientReponseObserver defined as interface, we have to declare member ourselves. (I don't know why)
            // although beforeStart is called before onNext, onNext is runned on thread pool, need to be volatile
            @volatile private var clientObserver: Option[ClientCallStreamObserver[DownloadRequest]] = None

            override def beforeStart(requestStream: ClientCallStreamObserver[DownloadRequest]): Unit = {
                clientObserver = Some(requestStream)
                requestStream.disableAutoRequestWithInitial(1)
            }

            override def onNext(response: DownloadResponse): Unit = {
                WorkerState.diskIoLock.synchronized {
                    blocking {
                        val writeBuffer = response.data.asReadOnlyByteBuffer()
                        while (writeBuffer.hasRemaining) {
                            fileChannel.write(writeBuffer)
                        }
                    }
                }
                assert { clientObserver.isDefined }
                clientObserver.get.request(1)
            }

            override def onError(e: Throwable): Unit = {  // error during streaming
                tryClose(fileChannel)
                promise.failure(e)
            }

            override def onCompleted(): Unit = {
                println(s"[$workerIp, $filename] response completed")
                tryClose(fileChannel)
                promise.success(())
            }
        }
        try {
            stub.downloadFile(DownloadRequest(filename = filename), observer)
        } catch {
            case e: Exception => {   // error on creating connection
                tryClose(fileChannel)
                promise.failure(e)
            }
        }

        promise.future
    }

    private def processFileWithRetry(workerIp: String, filename: String, tries: Int = 1): Future[Unit] = {
        processFile(workerIp, filename).recoverWith {
            case _ if tries < maxTries => {  // 방금 시도한게 n번째 시도이면 더이상 시도하지 않음
                println(s"Retrying [$workerIp, $filename], attempt #$tries")
                blocking { Thread.sleep(math.pow(2, tries).toLong * 1000) }
                processFileWithRetry(workerIp, filename, tries + 1)
            }
        }
    }
}