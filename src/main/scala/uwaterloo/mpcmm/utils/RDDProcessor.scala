package uwaterloo.mpcmm.utils

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD

import scala.util.Random

class RDDProcessor(p: Int) extends java.io.Serializable{
  var partitioner = new HashPartitioner(p)

  def processInitialRdd(rdd: RDD[String], transpose: Boolean): RDD[(Int, (Int, Double))] = {
    rdd.flatMap(
      line => {
        val pair = line.replaceAll(" ", "").substring(1, line.length - 1).split(",")
        if (transpose) {
          List(Tuple2(pair(1).toIntOption.getOrElse(0), (pair(0).toIntOption.getOrElse(-1), pair(2).toDoubleOption.getOrElse(-1))))
        } else {
          List(Tuple2(pair(0).toIntOption.getOrElse(0), (pair(1).toIntOption.getOrElse(-1), pair(2).toDoubleOption.getOrElse(-1))))
        }
      }
    )
  }

  def saltPartitions(rdd: RDD[(Int, (Int, Double))]): RDD[(Int, (Int, Double))] = {
    val rand = new Random
    rdd.map(row => {(rand.nextInt(partitioner.numPartitions), row)})
      .partitionBy(partitioner).map(_._2)
  }
}
