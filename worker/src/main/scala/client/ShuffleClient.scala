package client

import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import worker.Worker
import io.grpc.ManagedChannelBuilder
import shuffle.Shuffle.{ShuffleGrpc, DownloadRequest}
import scala.async.Async.{async, await}
import scala.util.Failure

class ShuffleClient(implicit ec: ExecutionContext) {
    val maxRetries = 10
    val poolSize = 50

    val allRequestDone = Promise[Unit]()

    // TODO: make it global
    val stubs = Worker.getWorkerInfo.map { case (ip, port) =>
        val channel = io.grpc.ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build()
        val stub = ShuffleGrpc.stub(channel)
        (ip, stub)
    }.toMap

    var currentPoolSize = poolSize
    var roundRobinIterator: Iterator[(String, String)] = Iterator.empty  // should initialized once at start

	def start(receiverFileInfo: Map[String, List[String]]): Future[Unit] = {  // TODO: make input as optional, if none, restore
        updateRoundRobinIterator(receiverFileInfo)
        expandRequestChain()
        allRequestDone.future
    }

    // whenever move iterator, get filename from next worker's iterator, and circulate
    private def updateRoundRobinIterator(receiverFileInfo: Map[String, List[String]]): Unit = {
        roundRobinIterator = for {
            iterators <- Iterator
                .continually(receiverFileInfo.mapValues(_.iterator).toList)  // List((w1, it1), (w2, it2)) => List(((w1, it1), (w2, it2)), ((w1, it1), (w2, it2)), ...)
                .takeWhile(_.exists { case (_, it) => it.hasNext })
            (worker, iterator) <- iterators
            if iterator.hasNext
        } yield (worker, iterator.next())
    }

    // chain: after process file, it finds another file to process
    // simultaneously, it immediately creates another chain by recurse so that number of chain becomes poolSize
    private def expandRequestChain(): Unit = {
        tryNextRequestWithPool() match {
            case None => ()  // end of current chain
            case Some((worker, filename)) => {
                // create new thread
                async {
                    await { processFile(worker, filename) }
                    if (releasePoolAndCheckAllRequestDone()) allRequestDone.success()
                    else expandRequestChain()  // create next ring of the chain after finishing current file
                }.onComplete {
                    case Failure(ex) => allRequestDone.tryFailure(ex)
                    case _ => ()
                }
                // immediately executed. try to increase parallelism. use tailrec
                expandRequestChain()  // create new chain
            }
        }
    }

    private def processFile(worker: String, filename: String, retries: Int = 1): Future[Unit] = {
        async {
            val stub = Worker.synchronized(stubs(worker))
            val bytes = await { stub.downloadFile(DownloadRequest(filename = filename)) }
            val targetPath = Paths.get(s"${Worker.shuffleDir}/$filename")
            val _ = blocking { Files.write(targetPath, bytes.data.toByteArray, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING) }
        }.recoverWith {
            case _ if retries <= maxRetries => {
                blocking { Thread.sleep(math.pow(2, retries).toLong) }
                processFile(worker, filename, retries + 1)
            }
        }
    }

    private def tryNextRequestWithPool() = this.synchronized {
        if (currentPoolSize > 0 && roundRobinIterator.hasNext) {
            currentPoolSize -= 1
            Some(roundRobinIterator.next())
        } else None
    }

    private def releasePoolAndCheckAllRequestDone() = this.synchronized {
        currentPoolSize += 1
        currentPoolSize == poolSize && !roundRobinIterator.hasNext
    }
}