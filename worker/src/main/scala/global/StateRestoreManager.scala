package global

import java.io._
import global.WorkerState
import utils.FileManager

object StateRestoreManager {
    val stateFileName: String = "worker_state"
    implicit val outputSubDir: FileManager.OutputSubDir = FileManager.OutputSubDir(FileManager.stateRestoreDirName)

    def isClean(): Boolean = this.synchronized {
        !new File(FileManager.getFilePathFromOutputDir(stateFileName)).exists()
    }

    def storeState(): Unit = this.synchronized {
        FileManager.createDirectoryIfNotExists(FileManager.getFilePathFromOutputDir(""))

        val oos = new ObjectOutputStream(new FileOutputStream(FileManager.getFilePathFromOutputDir(stateFileName)))
        try {
            val instance = WorkerState.synchronized { WorkerState.instance }
            oos.writeObject(instance)
        } finally {
            oos.close()
        }
    }

    def restoreState() = this.synchronized {
        assert(!isClean())

        val ois = new ObjectInputStream(new FileInputStream(FileManager.getFilePathFromOutputDir(stateFileName)))
        try {
            val instance = ois.readObject().asInstanceOf[WorkerState]
            instance.states.foreach(_.restoreTransient())
            WorkerState.synchronized { WorkerState.instance = instance }
        } finally {
            ois.close()
        }
    }

    def clear(): Unit = this.synchronized {
        FileManager.delete(FileManager.getFilePathFromOutputDir(stateFileName))
    }
}