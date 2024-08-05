package uwaterloo.mpcmm.matrix

import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.config.Configurator
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop._
import uwaterloo.mpcmm.utils.{RDDProcessor, Timer}

class DefaultMatrixMultConf (args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(R1Path,R2Path)
  val R1Path = opt[String](descr = "R1 path", required = true)
  val R2Path = opt[String](descr = "R2 path", required = true)
  val numReducers = opt[Int](descr = "number of reducers", required = true)
  val resultFolder = opt[String](descr = "folder for results", required = true)
  verify()
}

object DefaultMatrixMult {
  val logger = LogManager.getLogger(getClass().getName())
  val timer = new Timer()

  def main(argv: Array[String]) {
    Configurator.setLevel("uwaterloo.mpcmm", Level.OFF)
    Configurator.setLevel("org", Level.OFF)

    val args = new DefaultMatrixMultConf(argv)
//    logger.info("R1Path: " + args.R1Path())
//    logger.info("R2Path: " + args.R2Path())

    val conf = new SparkConf().setAppName("DefaultMatrixMult")
    val sc = new SparkContext(conf)

    val p = args.numReducers()
    val R1 = sc.textFile(args.R1Path() + "/part-00000", p);
    val R2 = sc.textFile(args.R2Path() + "/part-00000", p);
    val rddProcessor = new RDDProcessor(p)

//    For testing Light Light join
//    val R1processedLight = rddProcessor.processInitialRdd(R1, true).filter(_._2 != 0)
//    val R2processedLight = rddProcessor.processInitialRdd(R2, false).filter(_._2 != 4)
//
//    val resultLight = R1processedLight.join(R2processedLight)
//      .map(row => ((row._2._1._1, row._2._2._1), row._2._1._2 * row._2._2._2))
//      .reduceByKey(_+_).map(row => (row._1._1, row._1._2, row._2))
//    resultLight.saveAsTextFile("result/DefaultMatrixMultLightLight.txt")
//
//    val resultSortedLight = resultLight.sortBy(r => (r._1, r._2), numPartitions = 1)
//    resultSortedLight.saveAsTextFile("result/DefaultMatrixMultSortedLightLight.txt")
//    logger.info("resultsize: " + resultSortedLight.count())


    val R1processed = rddProcessor.processInitialRdd(R1, true)
    val R2processed = rddProcessor.processInitialRdd(R2, false)

    val R1partitioned = rddProcessor.saltPartitions(R1processed)
    val R2partitioned = rddProcessor.saltPartitions(R2processed)

    timer.start()
    val result = R1partitioned.join(R2partitioned)
      .map(row => ((row._2._1._1, row._2._2._1), row._2._1._2 * row._2._2._2))
      .reduceByKey(_+_).map(row => (row._1._1, row._1._2, row._2))
    timer.end()

    result.saveAsTextFile(args.resultFolder() + "/DefaultMatrixMult.txt")
    logger.info("result size: " + result.count())
    logger.info("join time in ms: " + timer.getElapsedTime())

    reflect.io.File(args.resultFolder() + "/DefaultSummary.txt")
      .writeAll("result size: " + result.count() + "\n" + "join time in ms: " + timer.getElapsedTime())

//    For testing result
//    val resultSorted = result.sortBy(r => (r._1, r._2), numPartitions = 1)
//    resultSorted.saveAsTextFile(args.resultFolder() + "/DefaultMatrixMultSorted.txt")
  }
}
