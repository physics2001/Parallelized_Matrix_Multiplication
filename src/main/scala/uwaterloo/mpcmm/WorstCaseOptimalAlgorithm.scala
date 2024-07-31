package uwaterloo.mpcmm

import org.apache.log4j.Logger
import org.apache.hadoop.fs._
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.rogach.scallop._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.joins.CartesianProductExec

import scala.util.Random
import scala.collection.{Map, immutable, mutable}

class WorstCaseOptimalAlgorithmConf (args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(R1Path,R2Path)
  val R1Path = opt[String](descr = "R1 path", required = true)
  val R2Path = opt[String](descr = "R2 path", required = true)
  val numReducers = opt[Int](descr = "number of reducers", required = true)
  verify()
}

object WorstCaseOptimalAlgorithm {

  val logger = Logger.getLogger(getClass().getName())

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

  def findSplitters(rdd: RDD[(Int, Int)], p: Int, N: Long): Seq[Int] = {
    val sampleSize = Math.ceil(Math.min(15 * Math.log(p) * p, N))
    val rddSample = rdd.sample(false, sampleSize / N)
    rddSample.sortBy(row => row._2, numPartitions=1)
      .zipWithIndex()
      .filter(_._2 % (Math.ceil(sampleSize/p)).toInt == 0).map(_._1._2).collect().drop(1)
  }

  def findWeightedRDD(sc: SparkContext, rdd: RDD[(Int, Int)], degreeMap: Map[Int, Long]): RDD[(Int, Int)] = {
    val keySetBc = sc.broadcast(degreeMap.keySet)
    rdd.filter(x => keySetBc.value.contains(x._1))
  }

  def assignPartitionNumberToHeavy(rdd: RDD[(Int, Int)], assignmentMapBc: Broadcast[mutable.Map[Int, (Int, Int)]]): RDD[(Int, (Int, Int))] = {
    rdd.map(row => {
      val keyVal = row._1
      val joinVal = row._2
      val pair = assignmentMapBc.value(keyVal)
      (joinVal % pair._2 + pair._1, (keyVal, joinVal))
    })
  }

  def assignPartitionNumberWithSplitters(rdd: RDD[(Int, Int)], splittersBc: Broadcast[Seq[Int]], start: Int): RDD[(Int, (Int, Int))] = {
    rdd.map(row => {
      val b = row._2
      val (firstHalf, secondHalf) = splittersBc.value.span(_ <= b)
      (firstHalf.size + start, row)
    })
  }

  def heavyLightParitionAssignment(sc: SparkContext, rddHeavy: RDD[(Int, Int)], rddLight: RDD[(Int, Int)],
                                   serverAssignmentHeavyLightMap: Map[Int, (Int, Int)]): (RDD[(Int, (Int, Int))], RDD[(Int, (Int, Int))]) = {
    val rddsHeavyAssigned = new mutable.ListBuffer[RDD[(Int, (Int, Int))]]
    val rddsLightAssigned = new mutable.ListBuffer[RDD[(Int, (Int, Int))]]

    serverAssignmentHeavyLightMap.foreach {
      case (key, pair) =>
        val rddWithKey = rddHeavy.filter(_._1 == key)
        val rddUnion = rddWithKey.union(rddLight)
        if (pair._2 >= 2) {
          val splitters = findSplitters(rddUnion, pair._2, rddUnion.count())
          val splittersBc = sc.broadcast(splitters)
          val rddHeavyAssigned = assignPartitionNumberWithSplitters(rddWithKey, splittersBc, pair._1)
          val rddLightAssigned = assignPartitionNumberWithSplitters(rddLight, splittersBc, pair._1)
          rddsHeavyAssigned.addOne(rddHeavyAssigned)
          rddsLightAssigned.addOne(rddLightAssigned)
        } else {
          val rddHeavyAssigned = rddWithKey.map(row => (pair._1, row))
          val rddLightAssigned = rddLight.map(row => (pair._1, row))
          rddsHeavyAssigned.addOne(rddHeavyAssigned)
          rddsLightAssigned.addOne(rddLightAssigned)
        }
    }
    (rddsHeavyAssigned.reduce(_ union _), rddsLightAssigned.reduce(_ union _))
  }

  def saltedPartition(rdd: RDD[(Int, Int)], partitioner: Partitioner): RDD[(Int, Int)] = {
    val rand = new Random
    rdd.map(row => {(rand.nextInt(partitioner.numPartitions), row)})
      .partitionBy(partitioner).map(_._2)
  }

  def main(argv: Array[String]) {
    val args = new WorstCaseOptimalAlgorithmConf(argv)
    logger.info("R1Path: " + args.R1Path())
    logger.info("R2Path: " + args.R2Path())

    val conf = new SparkConf().setAppName("WorstCaseOptimalAlgorithm")
    val sc = new SparkContext(conf)
    val p = args.numReducers()

    val R1 = sc.textFile(args.R1Path() + "/part-00000", p);
    val R2 = sc.textFile(args.R2Path() + "/part-00000", p);

    val R1processed = processInitialRdd(R1, false)
    val R2processed = processInitialRdd(R2, true)

    val partitioner = new HashPartitioner(p)
    val R1partitioned = saltedPartition(R1processed, partitioner)
    val R2partitioned = saltedPartition(R2processed, partitioner)

    val N1 = R1partitioned.count()
    val N2 = R2partitioned.count()
    val L = math.sqrt(N1 * N2 / p)
    logger.info("N1 size:" + N1)
    logger.info("N2 size:" + N2)
    logger.info("p size:" + p)
    logger.info("L size:" + L)

    val R1Counted = R1partitioned.countByKey()
    val R2Counted = R2partitioned.countByKey()

    val R1AHeavyDegree = R1Counted.filter(_._2 > L)
    val R1AHeavy = findWeightedRDD(sc, R1partitioned, R1AHeavyDegree)
    val R1ALightDegree = R1Counted.filter(_._2 <= L)
    val R1ALightCount = R1ALightDegree.size
    val R1ALight = findWeightedRDD(sc, R1partitioned, R1ALightDegree)

    val R2CHeavyDegree = R2Counted.filter(_._2 > L)
    val R2CHeavy = findWeightedRDD(sc, R2partitioned, R2CHeavyDegree)
    val R2CLightDegree = R2Counted.filter(_._2 <= L)
    val R2CLightCount = R2CLightDegree.size
    val R2CLight = findWeightedRDD(sc, R2partitioned, R2CLightDegree)

    val AHeavyCHeavyServerCount = mutable.Map[(Int, Int), Int]()
    R1AHeavyDegree.foreach(x => {
      R2CHeavyDegree.foreach(y => {
        AHeavyCHeavyServerCount.put((x._1, y._1), math.ceil((x._2 + y._2) / L).toInt)
      })
    })

    val AHeavyCLightServerCount = mutable.Map[Int, Int]()
    R1AHeavyDegree.foreach(x => {
      AHeavyCLightServerCount.put(x._1, math.ceil((x._2 + R2CLightCount) / L).toInt)
    })

    val ALightCHeavyServerCount = mutable.Map[Int, Int]()
    R2CHeavyDegree.foreach(y => {
      ALightCHeavyServerCount.put(y._1, math.ceil((y._2 + R1ALightCount) / L).toInt)
    })

    val serverUsageHeavyHeavy = AHeavyCHeavyServerCount
    var start = 0
    val serverAssignmentHeavyHeavy = serverUsageHeavyHeavy.map(row => {
        start += row._2
        (row._1, (start - row._2, row._2))
      }
    )

    val serverAssignmentAHeavy = serverAssignmentHeavyHeavy.map(row => (row._1._1, row._2))
    val serverAssignmentAHeavyCLight = AHeavyCLightServerCount.map(
      row => {
        start += row._2
        (row._1, (start - row._2, row._2))
      }
    )

    val serverAssignmentCHeavy = serverAssignmentHeavyHeavy.map(row => (row._1._2, row._2))
    val serverAssignmentALightCHeavy = ALightCHeavyServerCount.map(
      row => {
        start += row._2
        (row._1, (start - row._2, row._2))
      }
    )

    val serverAssignmentAHeavyBc = sc.broadcast(serverAssignmentAHeavy)
    val serverAssignmentCHeavyBc = sc.broadcast(serverAssignmentCHeavy)


    val R1AHeavyAssigned = assignPartitionNumberToHeavy(R1AHeavy, serverAssignmentAHeavyBc)
    val R2CHeavyAssigned = assignPartitionNumberToHeavy(R2CHeavy, serverAssignmentCHeavyBc)

    val (r1AHeavyCLightAssigned, r2CLightAHeavyAssigned)
    = heavyLightParitionAssignment(sc, R1AHeavy, R2CLight, serverAssignmentAHeavyCLight)

    val (r2CHeavyALightAssigned, r1ALightCHeavyAssigned)
    = heavyLightParitionAssignment(sc, R2CHeavy, R1ALight, serverAssignmentALightCHeavy)

    val R1Assigned = R1AHeavyAssigned.union(r1AHeavyCLightAssigned).union(r1ALightCHeavyAssigned).coalesce(p)
    val R2Assigned = R2CHeavyAssigned.union(r2CHeavyALightAssigned).union(r2CLightAHeavyAssigned).coalesce(p)

    val notLightLightJoinResult = R1Assigned.join(R2Assigned)
    val notLightLightJoinResultValidated = notLightLightJoinResult.filter(row => row._2._1._2 == row._2._2._2)

    notLightLightJoinResultValidated.saveAsTextFile("result/WorstCaseJoinNotLightLight.txt")
    logger.info("validatedJoinResultNotLightLight size: " + notLightLightJoinResultValidated.count())

    notLightLightJoinResultValidated.map(
        row => (row._2._1._2, (row._2._1._1, row._2._2._1))
      ).sortBy(r => (r._1, r._2._1, r._2._2), numPartitions = 1)
      .saveAsTextFile("result/WorstCaseJoinNotLightLightSorted.txt")

    val k = math.ceil(N1 / L).toInt
    var serverALightLoads = Vector.fill(k)(0L)
    val lightAKeyAssignment: mutable.Map[Int, Int] = mutable.Map()
    R1ALightDegree.toSeq.sortBy(_._2).foreach(
      row => {
        val minLoad = serverALightLoads.min
        val minIndex = serverALightLoads.indexOf(minLoad)
        serverALightLoads = serverALightLoads.updated(minIndex, minLoad + row._2)
        lightAKeyAssignment.put(row._1, minIndex)
      }
    )
    val lightAKeyAssignmentBc =  sc.broadcast(lightAKeyAssignment)

    val l = math.ceil(N2 / L).toInt
    var serverCLightLoads = Vector.fill(l)(0L)
    val lightCKeyAssignment: mutable.Map[Int, Int] = mutable.Map()
    R2CLightDegree.toSeq.sortBy(_._2).foreach(row => {
      val minLoad = serverCLightLoads.min
      val minIndex = serverCLightLoads.indexOf(minLoad)
      serverCLightLoads = serverCLightLoads.updated(minIndex, minLoad + row._2)
      lightCKeyAssignment.put(row._1, minIndex)
    })
    val lightCKeyAssignmentBc =  sc.broadcast(lightCKeyAssignment)

    logger.info("k size:" + k)
    logger.info("l size:" + l)

    val R1ALightwithPartition = R1ALight.flatMap(row => {
      val result = mutable.ListBuffer[(Int, (Int, Int))]()
      val AVal = row._1
      val BVal = row._2
      val part = lightAKeyAssignmentBc.value(AVal)
      for (i <- 0 until l) {
        result += ((k * i + part, (AVal, BVal)))
      }
      result
    })
    R1ALightwithPartition.foreach(println)

    val R2CLightwithPartition = R2CLight.flatMap(row => {
      val result = mutable.ListBuffer[(Int, (Int, Int))]()
      val CVal = row._1
      val BVal = row._2
      val part = lightCKeyAssignmentBc.value(CVal)
      for (i <- 0 until k) {
        result += ((l * i + part, (CVal, BVal)))
      }
      result
    })
    R2CLightwithPartition.foreach(println)

    val lightLightJoinResult = R1ALightwithPartition.join(R2CLightwithPartition)
    val lightLightJoinResultValidated = lightLightJoinResult.filter(row => row._2._1._2 == row._2._2._2)

    lightLightJoinResultValidated.saveAsTextFile("result/WorstCaseJoinLightLight.txt")
    logger.info("validatedJoinResultLightLight size: " + lightLightJoinResultValidated.count())

    lightLightJoinResultValidated.map(
        row => (row._2._1._2, (row._2._1._1, row._2._2._1))
      ).sortBy(r => (r._1, r._2._1, r._2._2), numPartitions = 1)
      .saveAsTextFile("result/WorstCaseJoinLightLightSorted.txt")

    val result = lightLightJoinResultValidated.union(notLightLightJoinResultValidated).coalesce(p).map(
      row => (row._2._1._2, (row._2._1._1, row._2._2._1))
    )
    result.saveAsTextFile("result/WorstCaseJoin.txt")

    val resultSorted = result.sortBy(r => (r._1, r._2._1, r._2._2), numPartitions = 1)
    resultSorted.saveAsTextFile("result/WorstCaseJoinSorted.txt")
    logger.info("Total result size: " + resultSorted.count())
  }
}
