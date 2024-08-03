package uwaterloo.mpcmm.matrix

import org.apache.commons.math3.distribution.PoissonDistribution
import org.apache.spark.sql.SparkSession

import scala.util.Random


object RandomSparseMatrixGenerator {
  val spark: SparkSession = SparkSession.builder()
    .master("local[10]")
    .appName("RandomSparseMatrixGenerator")
    .getOrCreate()

  def main(args:Array[String]): Unit = {
    val rand = Random
    val listN = List(1000, 10000, 100000, 1000000, 2000000)
    for (n <- listN) {
      val matrixSize = n / 4
      val poisson = new PoissonDistribution(matrixSize / 4d)
      val joinVals = poisson.sample(n).map(ind => math.min(ind, matrixSize-1))
      val data = joinVals.map(x => (rand.nextInt(matrixSize), x+1))

      spark.sparkContext.parallelize(data).distinct()
        .map(row => (row._1, row._2, rand.nextDouble()))
        .saveAsTextFile("data/matrix/InSize"+n)
    }
  }
}
