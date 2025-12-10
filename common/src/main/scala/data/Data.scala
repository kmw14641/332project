package common.data

import scala.math.Ordering

object Data {
  type Record = Array[Byte]

  val RECORD_SIZE = 100
  val KEY_SIZE = 10
  val VALUE_SIZE = 90

  val getRecordOrdering: Ordering[Record] = {
    new Ordering[Record] {
      override def compare(x: Record, y: Record): Int = {
        var i = 0
        while (i < KEY_SIZE) {
          val a = x(i) & 0xFF
          val b = y(i) & 0xFF
          if (a != b) {
            return a - b
          }
          i += 1
        }
        0
      }
    }
  }

  val getKeyOrdering: Ordering[Array[Byte]] = {
    new Ordering[Array[Byte]] {
      override def compare(x: Array[Byte], y: Array[Byte]): Int = {
        val len = Math.min(x.length, y.length)
        var i = 0
        while (i < KEY_SIZE) {
          val a = x(i) & 0xFF
          val b = y(i) & 0xFF
          if (a != b) {
            return a - b
          }
          i += 1
        }
        x.length - y.length
      }
    }
  }
}
