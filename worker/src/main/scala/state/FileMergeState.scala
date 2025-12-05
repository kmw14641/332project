package state

import global.{Restorable, WorkerState}

class FileMergeState extends Serializable with Restorable {
  // Merge Sort State
  var currentRound: Int = 1
  var currentFileLists: Option[List[List[String]]] = None
  var completedPairs: Map[Int, List[String]] = Map.empty

  def restoreTransient(): Unit = {}
}

object FileMergeState {
  // Helper methods to access state instance
  def getCurrentFileLists(implicit state: FileMergeState): Option[List[List[String]]] = WorkerState.synchronized {
    state.currentFileLists
  }
  
  def setCurrentFileLists(lists: List[List[String]])(implicit state: FileMergeState): Unit = WorkerState.synchronized {
    state.currentFileLists = Some(lists)
  }
  
  def getRound(implicit state: FileMergeState): Int = WorkerState.synchronized {
    state.currentRound
  }
  
  def setRound(r: Int)(implicit state: FileMergeState): Unit = WorkerState.synchronized {
    state.currentRound = r
  }
  
  def isPairCompleted(index: Int)(implicit state: FileMergeState): Boolean = WorkerState.synchronized {
    state.completedPairs.contains(index)
  }
  
  def getCompletedPairResult(index: Int)(implicit state: FileMergeState): List[String] = WorkerState.synchronized {
    state.completedPairs(index)
  }
  
  def markPairCompleted(index: Int, result: List[String])(implicit state: FileMergeState): Unit = WorkerState.synchronized {
    state.completedPairs += (index -> result)
  }
  
  def clearCompletedPairs()(implicit state: FileMergeState): Unit = WorkerState.synchronized {
    state.completedPairs = Map.empty
  }
}
