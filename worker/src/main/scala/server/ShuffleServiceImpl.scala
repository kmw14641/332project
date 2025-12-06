package server

import org.slf4j.LoggerFactory
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.{ExecutionContext, Future, blocking}
import worker.WorkerService.{DownloadRequest, DownloadResponse, ShuffleServiceGrpc}
import com.google.protobuf.ByteString
import global.WorkerState
import java.io.InputStream
import scala.util.Using
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import io.grpc.stub.{StreamObserver, ServerCallStreamObserver}
import java.util.concurrent.atomic.AtomicBoolean
import utils.FileManager
import global.GlobalLock
import utils.FileManager.InputSubDir

class ShuffleServiceImpl(inputSubDirName: String)(implicit ec: ExecutionContext) extends ShuffleServiceGrpc.ShuffleService {
    private val logger = LoggerFactory.getLogger(getClass)
    implicit val inputSubDirNameImplicit: InputSubDir = InputSubDir(inputSubDirName)
    val chunkSize = 1024 * 1024 * 180

    override def downloadFile(request: DownloadRequest, responseObserver: StreamObserver[DownloadResponse]): Unit = {
        val sourcePath = Paths.get(FileManager.getFilePathFromInputDir(request.filename))
        val serverObserver = responseObserver.asInstanceOf[ServerCallStreamObserver[DownloadResponse]]
        val fileChannel = FileChannel.open(sourcePath, StandardOpenOption.READ)  // 파일 열기 실패 시 StatusException 발생

        val done = new AtomicBoolean(false)

        def tryClose(fileChannel: FileChannel): Unit = {
            try { fileChannel.close() } catch {
                case e: Throwable => logger.warn(s"Failed to close fileChannel for ${request.filename}: ${e.getMessage}")
            }
        }

        val fileSize = fileChannel.size()
        val buffer = try { ByteBuffer.allocateDirect(math.min(chunkSize, fileSize.toInt)) } catch {
            case e: Throwable =>
                tryClose(fileChannel)
                throw e
        }

        serverObserver.setOnCancelHandler(new Runnable {
            override def run(): Unit = {
                done.set(true)
                tryClose(fileChannel)
                // cancel에서 onError 호출 불가능 (gpt 피셜)
            }
        })

        serverObserver.setOnReadyHandler(new Runnable {
            override def run(): Unit = {
                try {
                    // serverObserver.isReady는 onCompleted 호출 여부와 무관함.
                    // setReadyHandler(null)을 해도 그전에 Runnable이 등록된 상태일 수 있음
                    // 따라서 done이 필요함. (Runnable은 동시에 실행되지 않아서 lock은 없어도 됨 (gpt 피셜))
                    while (serverObserver.isReady && !done.get()) {
                        buffer.clear()
                        val bytesRead = GlobalLock.diskIoLock.synchronized {
                            fileChannel.read(buffer)
                        }
                        if (bytesRead == -1) {
                            done.set(true)
                            responseObserver.onCompleted()
                            tryClose(fileChannel)
                        } else {
                            buffer.flip()
                            responseObserver.onNext(DownloadResponse(data = ByteString.copyFrom(buffer)))
                        }
                    }
                } catch {
                    case e: Throwable =>
                        done.set(true)
                        responseObserver.onError(e)
                        tryClose(fileChannel)
                }
            }
        })
    }
}

