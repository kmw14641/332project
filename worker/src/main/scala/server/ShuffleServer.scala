package server

import java.nio.file.{Files, Paths}
import scala.concurrent.{ExecutionContext, Future, blocking}
import shuffle.Shuffle.{DownloadRequest, DownloadResponse, ShuffleGrpc}
import com.google.protobuf.ByteString
import worker.Worker

class ShuffleServiceImpl(implicit ec: ExecutionContext) extends ShuffleGrpc.Shuffle {
	override def downloadFile(request: DownloadRequest): Future[DownloadResponse] = Future {
        val sourcePath = Paths.get(s"${Worker.firstMergeDir}/${request.filename}")
        val bytes = blocking { Files.readAllBytes(sourcePath) }
        DownloadResponse(data = ByteString.copyFrom(bytes))  // creates 3 copy. os heap -> jvm heap -> bytestring -> netty buffer
    }
}

