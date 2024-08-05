package uwaterloo.mpcmm.matrix

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.spark.sql.SparkSession

import scala.util.Random
import scala.collection.parallel.immutable.ParSeq

object RandomSparseMatrixGenerator {
  val spark: SparkSession = SparkSession.builder()
    .master("local[10]")
    .appName("RandomSparseMatrixGenerator")
    .getOrCreate()
  val sc = spark.sparkContext

  def main(args:Array[String]): Unit = {
    val rand = Random
    val listN = ParSeq(1000, 10000, 100000, 1000000, 2000000, 4000000)
    val nameList = ParSeq("R1", "R2")
    listN.foreach(n => {
      nameList.foreach(name => {
        val matrixSize = n / 4
        val normalDistribution = new NormalDistribution(matrixSize / 2d, matrixSize / 4d)
        val joinVals = sc.parallelize(normalDistribution.sample(n).map(ind => math.max(math.min(ind.toInt, matrixSize), 1)))
        val keyVals = sc.parallelize(normalDistribution.sample(n).map(ind => math.max(math.min(ind.toInt, matrixSize), 1)))
        keyVals.zip(joinVals).distinct()
          .map(row => (row._1, row._2, rand.nextDouble()))
          .saveAsTextFile("data/matrix/InSize"+n+name)
      })
    })
  }
}
