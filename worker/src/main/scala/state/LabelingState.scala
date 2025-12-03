package state

import global.WorkerState

import global.Restorable

class LabelingState extends Serializable with Restorable {
  private var assignedFiles: Map[(String, Int), List[String]] = Map.empty

  def restoreTransient(): Unit = {}
}

object LabelingState {
  def setAssignedFiles(files: Map[(String, Int), List[String]]): Unit = WorkerState.synchronized {
    WorkerState.labeling.assignedFiles = files
  }

  def getAssignedFiles: Map[(String, Int), List[String]] = WorkerState.synchronized {
    WorkerState.labeling.assignedFiles
  }
}