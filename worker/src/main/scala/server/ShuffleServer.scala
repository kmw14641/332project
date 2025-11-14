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
import io.grpc.stub.StreamObserver

class ShuffleServiceImpl(implicit ec: ExecutionContext) extends ShuffleGrpc.Shuffle {
    val chunkSize = 1024 * 1024 * 1

	override def downloadFile(request: DownloadRequest, responseObserver: StreamObserver[DownloadResponse]): Unit = {
        val sourcePath = Paths.get(s"${Worker.mergeDir}/${request.filename}")

        Using(FileChannel.open(sourcePath, StandardOpenOption.READ)) { channel =>
            val buffer = ByteBuffer.allocateDirect(chunkSize)  // TODO: consider small files
            var bytesRead = channel.read(buffer)

            while (bytesRead != -1) {
                buffer.flip()  // 읽기 모드로 전환 (channel이 buffer의 끝에 seek해놓은 상태임)
                // TODO: onNext는 queue에 파일을 쌓아놓고 바로 return함. 흐름 제어 필요.
                responseObserver.onNext(DownloadResponse(data = ByteString.copyFrom(buffer)))
                buffer.clear()
                bytesRead = channel.read(buffer)
            }

            responseObserver.onCompleted()
        }.recover {
            case e: Exception =>
            responseObserver.onError(e)
        }
    }
}

