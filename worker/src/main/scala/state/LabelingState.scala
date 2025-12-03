package state

import global.WorkerState

class LabelingState extends Serializable {
  private var assignedFiles: Map[(String, Int), List[String]] = Map.empty
}

object LabelingState {
  def setAssignedFiles(files: Map[(String, Int), List[String]]): Unit = WorkerState.synchronized {
    WorkerState.labeling.assignedFiles = files
  }

  def getAssignedFiles: Map[(String, Int), List[String]] = WorkerState.synchronized {
    WorkerState.labeling.assignedFiles
  }
}