package global

import java.io._
import global.WorkerState
import utils.FileManager

object StateRestoreManager {
    val stateFileName: String = "worker_state"
    def fileDir: String = s"${FileManager.getOutputDir.get}/${FileManager.stateRestoreDirName}"
    def filePath: String = s"$fileDir/$stateFileName"

    def isClean(): Boolean = this.synchronized {
        !new File(filePath).exists()
    }

    def storeState(): Unit = this.synchronized {
        FileManager.createDirectoryIfNotExists(fileDir)

        val oos = new ObjectOutputStream(new FileOutputStream(filePath))
        try {
            val instance = WorkerState.synchronized { WorkerState.instance }
            oos.writeObject(instance)
        } finally {
            oos.close()
        }
    }

    def restoreState() = this.synchronized {
        assert(!isClean())

        val ois = new ObjectInputStream(new FileInputStream(filePath))
        try {
            val instance = ois.readObject().asInstanceOf[WorkerState]
            instance.states.foreach(_.restoreTransient())
            WorkerState.synchronized { WorkerState.instance = instance }
        } finally {
            ois.close()
        }
    }

    def clear(): Unit = this.synchronized {
        val file = new File(filePath)
        if (file.exists()) file.delete()
    }
}