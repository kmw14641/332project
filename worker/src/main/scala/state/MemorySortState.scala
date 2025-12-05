package state

import global.{Restorable, WorkerState}

class MemorySortState extends Serializable with Restorable {
  // Input file path -> List of generated sorted chunk file paths
  var processedInputFiles: Map[String, List[String]] = Map.empty

  def restoreTransient(): Unit = {}
}

object MemorySortState {
  def isFileProcessed(inputPath: String): Boolean = WorkerState.synchronized {
    WorkerState.memorySort.processedInputFiles.contains(inputPath)
  }

  def getProcessedFiles(inputPath: String): List[String] = WorkerState.synchronized {
    WorkerState.memorySort.processedInputFiles.getOrElse(inputPath, List.empty)
  }

  def markFileProcessed(inputPath: String, outputFiles: List[String]): Unit = WorkerState.synchronized {
    WorkerState.memorySort.processedInputFiles += (inputPath -> outputFiles)
  }
}
