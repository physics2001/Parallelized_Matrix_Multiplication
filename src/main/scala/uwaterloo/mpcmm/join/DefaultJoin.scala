package uwaterloo.mpcmm.join

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.rdd.RDD

class DefaultJoinConf (args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(R1Path,R2Path)
  val R1Path = opt[String](descr = "R1 path", required = true)
  val R2Path = opt[String](descr = "R2 path", required = true)
  val numReducers = opt[Int](descr = "number of reducers", required = true)
  val resultFolder = opt[String](descr = "folder for results", required = true)
  verify()
}

object DefaultJoin {
  val logger = LogManager.getLogger(getClass().getName())

  def processInitialRdd(rdd: RDD[String], transpose: Boolean): RDD[(Int, Int)] = {
    rdd.flatMap(
      line => {
        val pair = line.replaceAll(" ", "").substring(1, line.length - 1).split(",")
        if (transpose) {
          List(Tuple2(pair(1).toIntOption.getOrElse(0), pair(0).toIntOption.getOrElse(-1)))
        } else {
          List(Tuple2(pair(0).toIntOption.getOrElse(0), pair(1).toIntOption.getOrElse(-1)))
        }
      }
    )
  }

  def main(argv: Array[String]) {
    Configurator.setLevel("uwaterloo.mpcmm", Level.OFF)
    Configurator.setLevel("org", Level.OFF)

    val args = new DefaultJoinConf(argv)
    logger.info("R1Path: " + args.R1Path())
    logger.info("R2Path: " + args.R2Path())

    val conf = new SparkConf().setAppName("DefaultJoin")
    val sc = new SparkContext(conf)

    val R1 = sc.textFile(args.R1Path() + "/part-00000", args.numReducers());
    val R2 = sc.textFile(args.R2Path() + "/part-00000", args.numReducers());

//    For testing purpose only
//    val R1processedLight = processInitialRdd(R1, true).filter(_._2 != 0)
//    val R2processedLight = processInitialRdd(R2, false).filter(_._2 != 4)
//
//    val resultLight = R1processedLight.join(R2processedLight)
//    resultLight.saveAsTextFile(args.resultFolder() + "/DefaultJoinLightLight.txt")
//
//    val resultSortedLight = resultLight.sortBy(r => (r._1, r._2._1, r._2._2), numPartitions = 1)
//    resultSortedLight.saveAsTextFile(args.resultFolder() + "/DefaultJoinSortedLightLight.txt")
//    logger.info("resultsize: " + resultSortedLight.count())

    val R1processed = processInitialRdd(R1, true)
    val R2processed = processInitialRdd(R2, false)

    val result = R1processed.join(R2processed)
    result.saveAsTextFile(args.resultFolder() + "/DefaultJoin.txt")

    reflect.io.File(args.resultFolder() + "/DefaultSummary.txt")
      .writeAll("result size: " + result.count())

//    For testing purpose only
//    val resultSorted = result.sortBy(r => (r._1, r._2._1, r._2._2), numPartitions = 1)
//    resultSorted.saveAsTextFile(args.resultFolder() + "/DefaultJoinSorted.txt")
//    logger.info("resultsize: " + resultSorted.count())
//
//    resultSorted.map(row => row._2).distinct().sortBy(r => (r._1, r._2), numPartitions = 1)
//      .saveAsTextFile(args.resultFolder() + "/DefaultJoinDistinct.txt")
  }
}
