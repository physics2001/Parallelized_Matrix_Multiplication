package uwaterloo.mpcmm.matrix

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.rogach.scallop._

class DefaultMatrixMultConf (args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(R1Path,R2Path)
  val R1Path = opt[String](descr = "R1 path", required = true)
  val R2Path = opt[String](descr = "R2 path", required = true)
  val numReducers = opt[Int](descr = "number of reducers", required = true)
  verify()
}

object DefaultMatrixMult {
  val logger = Logger.getLogger(getClass().getName())

  def processInitialRdd(rdd: RDD[String], transpose: Boolean): RDD[(Int, Int, Double)] = {
    rdd.flatMap(
      line => {
        val pair = line.replaceAll(" ", "").substring(1, line.length - 1).split(",")
        if (transpose) {
          List(Tuple3(pair(1).toIntOption.getOrElse(0), pair(0).toIntOption.getOrElse(-1), pair(2).toDoubleOption.getOrElse(-1)))
        } else {
          List(Tuple3(pair(0).toIntOption.getOrElse(0), pair(1).toIntOption.getOrElse(-1), pair(2).toDoubleOption.getOrElse(-1)))
        }
      }
    )
  }

  def main(argv: Array[String]) {
    val args = new DefaultMatrixMultConf(argv)
    logger.info("R1Path: " + args.R1Path())
    logger.info("R2Path: " + args.R2Path())

    val conf = new SparkConf().setAppName("DefaultMatrixMult")
    val sc = new SparkContext(conf)

    val R1 = sc.textFile(args.R1Path() + "/part-00000", args.numReducers());
    val R2 = sc.textFile(args.R2Path() + "/part-00000", args.numReducers());

    val R1processedLight = processInitialRdd(R1, true).filter(_._2 != 0).map(row => (row._1, (row._2, row._3)))
    val R2processedLight = processInitialRdd(R2, false).filter(_._2 != 4).map(row => (row._1, (row._2, row._3)))

    val resultLight = R1processedLight.join(R2processedLight)
      .map(row => ((row._2._1._1, row._2._2._1), row._2._1._2 * row._2._2._2))
      .reduceByKey(_+_).map(row => (row._1._1, row._1._2, row._2))
    resultLight.saveAsTextFile("result/DefaultMatrixMultLightLight.txt")

    val resultSortedLight = resultLight.sortBy(r => (r._1, r._2), numPartitions = 1)
    resultSortedLight.saveAsTextFile("result/DefaultMatrixMultSortedLightLight.txt")
    logger.info("resultsize: " + resultSortedLight.count())

    val R1processed = processInitialRdd(R1, true).map(row => (row._1, (row._2, row._3)))
    val R2processed = processInitialRdd(R2, false).map(row => (row._1, (row._2, row._3)))

    val result = R1processed.join(R2processed)
      .map(row => ((row._2._1._1, row._2._2._1), row._2._1._2 * row._2._2._2))
      .reduceByKey(_+_).map(row => (row._1._1, row._1._2, row._2))
    result.saveAsTextFile("result/DefaultMatrixMult.txt")

    val resultSorted = result.sortBy(r => (r._1, r._2), numPartitions = 1)
    resultSorted.saveAsTextFile("result/DefaultMatrixMultSorted.txt")
    logger.info("resultsize: " + resultSorted.count())
  }
}
