package global

import scala.concurrent.{Future, Promise}
import common.data.Data.Key
import com.google.protobuf.ByteString
import state.{SampleState, LabelingState, SynchronizationState, TerminationState}
import state.ShuffleState

trait Restorable {
  def restoreTransient(): Unit
}

// get lock on WorkerState when access any data!

class WorkerState extends Serializable {
  val sample: SampleState = new SampleState()
  val labeling: LabelingState = new LabelingState()
  val synchronization: SynchronizationState = new SynchronizationState()
  val shuffle: ShuffleState = new ShuffleState()
  val termination: TerminationState = new TerminationState()

  def states: Seq[Restorable] = Seq(sample, labeling, synchronization, shuffle, termination)
}

object WorkerState {
  var instance: WorkerState = new WorkerState()

  def sample = instance.sample
  def labeling = instance.labeling
  def synchronization = instance.synchronization
  def shuffle = instance.shuffle
  def terminate = instance.termination
}
