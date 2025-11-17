package server

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.{ExecutionContext, Future, blocking}
import shuffle.Shuffle.{DownloadRequest, DownloadResponse, ShuffleGrpc}
import com.google.protobuf.ByteString
import worker.Worker
import java.io.InputStream
import scala.util.Using
import java.nio.channels.FileChannel
import java.nio.ByteBuffer
import io.grpc.stub.{StreamObserver, ServerCallStreamObserver}

class ShuffleServiceImpl(implicit ec: ExecutionContext) extends ShuffleGrpc.Shuffle {
    override def downloadFile(request: DownloadRequest, responseObserver: StreamObserver[DownloadResponse]): Unit = {
        val sourcePath = Paths.get(s"${Worker.mergeDir}/${request.filename}")
        val chunkSize = 1024 * 1024 * 180
        val serverObserver = responseObserver.asInstanceOf[ServerCallStreamObserver[DownloadResponse]]
        val fileChannel = FileChannel.open(sourcePath, StandardOpenOption.READ)  // 파일 열기 실패 시 StatusException 발생

        var done = false

        def tryClose(fileChannel: FileChannel): Unit = {
            try { fileChannel.close() } catch {
                case e: Throwable => println(s"[WARN] Failed to close fileChannel for ${request.filename}: ${e.getMessage}")
            }
        }

        val buffer = try { ByteBuffer.allocateDirect(chunkSize) } catch {
            case e: Throwable =>
                tryClose(fileChannel)
                throw e
        }

        serverObserver.setOnCancelHandler(new Runnable {
            override def run(): Unit = {
                done = true
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
                    while (serverObserver.isReady && !done) {
                        buffer.clear()
                        val bytesRead = Worker.diskIoLock.synchronized {
                            fileChannel.read(buffer)
                        }
                        if (bytesRead == -1) {
                            done = true
                            responseObserver.onCompleted()
                            tryClose(fileChannel)
                        } else {
                            buffer.flip()
                            responseObserver.onNext(DownloadResponse(data = ByteString.copyFrom(buffer)))
                        }
                    }
                } catch {
                    case e: Throwable =>
                        done = true
                        responseObserver.onError(e)
                        tryClose(fileChannel)
                }
            }
        })
    }
}

