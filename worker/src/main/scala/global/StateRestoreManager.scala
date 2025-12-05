package global

import java.io._
import global.WorkerState
import utils.FileManager
import scala.util.Using

object StateRestoreManager {
    val stateFileName: String = "worker_state"
    implicit val outputSubDir: FileManager.OutputSubDir = FileManager.OutputSubDir(FileManager.stateRestoreDirName)

    def isClean(): Boolean = this.synchronized {
        !new File(FileManager.getFilePathFromOutputDir(stateFileName)).exists()
    }

    def storeState(): Unit = this.synchronized {
        FileManager.createDirectoryIfNotExists(FileManager.getFilePathFromOutputDir(""))

        val bos = new ByteArrayOutputStream()
        Using(new ObjectOutputStream(bos)) { oos =>
            WorkerState.synchronized { oos.writeObject(WorkerState.instance) }  // serialize to memory first, release lock, write to disk
        }.get

        Using(new FileOutputStream(FileManager.getFilePathFromOutputDir(stateFileName))) { fos =>
            fos.write(bos.toByteArray)
        }.get
    }

    def restoreState() = this.synchronized {
        assert(!isClean())

        Using(new ObjectInputStream(new FileInputStream(FileManager.getFilePathFromOutputDir(stateFileName)))) { ois =>
            val instance = ois.readObject().asInstanceOf[WorkerState]
            instance.states.foreach(_.restoreTransient())
            WorkerState.synchronized { WorkerState.instance = instance }  // just replace already created instance
        }.get
    }

    def clear(): Unit = this.synchronized {
        FileManager.delete(FileManager.getFilePathFromOutputDir(stateFileName))
    }
}