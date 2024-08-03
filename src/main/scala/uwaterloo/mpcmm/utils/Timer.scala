package uwaterloo.mpcmm.utils

class Timer extends java.io.Serializable{
  var time_i = 0L
  var time_f = 0L

  def start() = {
    time_i = System.nanoTime / 1000000
  }

  def end() = {
    time_f = System.nanoTime / 1000000
  }

  def reset() = {
    time_i = 0L
    time_f = 0L
  }

  def getElapsedTime() = {
    time_f - time_i
  }
}
