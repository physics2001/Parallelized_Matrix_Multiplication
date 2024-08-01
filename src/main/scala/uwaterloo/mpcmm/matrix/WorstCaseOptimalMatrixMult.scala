package uwaterloo.mpcmm.matrix

import org.apache.log4j.Logger
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.rogach.scallop._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.util.Random
import scala.collection.{Map, immutable, mutable}

class WorstCaseOptimalMatrixMultConf (args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(R1Path,R2Path)
  val R1Path = opt[String](descr = "R1 path", required = true)
  val R2Path = opt[String](descr = "R2 path", required = true)
  val numReducers = opt[Int](descr = "number of reducers", required = true)
  verify()
}

object WorstCaseOptimalMatrixMult {

  val logger = Logger.getLogger(getClass().getName())

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

  def findSplitters(rdd: RDD[(Int, (Int, Double))], p: Int, N: Long): Seq[Int] = {
    val sampleSize = Math.ceil(Math.min(15 * Math.log(p) * p, N))
    val rddSample = rdd.sample(false, sampleSize / N).map(row=>(row._1, row._2._1))
    rddSample.sortBy(row => row._2, numPartitions=1)
      .zipWithIndex()
      .filter(_._2 % (Math.ceil(sampleSize/p)).toInt == 0).map(_._1._2).collect().drop(1)
  }

  def findWeightedRDD(sc: SparkContext, rdd: RDD[(Int, (Int, Double))], degreeMap: Map[Int, Long]): RDD[(Int, (Int, Double))] = {
    val keySetBc = sc.broadcast(degreeMap.keySet)
    rdd.filter(x => keySetBc.value.contains(x._1))
  }

  def assignPartitionNumberToHeavy(rdd: RDD[(Int, (Int, Double))], assignmentMapBc: Broadcast[mutable.Map[Int, (Int, Int)]]): RDD[(Int, (Int, (Int, Double)))] = {
    rdd.map(row => {
      val keyVal = row._1
      val joinVal = row._2._1
      val weight = row._2._2
      val pair = assignmentMapBc.value(keyVal)
      (joinVal % pair._2 + pair._1, (keyVal, (joinVal, weight)))
    })
  }

  def assignPartitionNumberWithSplitters(rdd: RDD[(Int, (Int, Double))], splittersBc: Broadcast[Seq[Int]],
                                         start: Int): RDD[(Int, (Int, (Int, Double)))] = {
    rdd.map(row => {
      val b = row._2._1
      val (firstHalf, secondHalf) = splittersBc.value.span(_ <= b)
      (firstHalf.size + start, row)
    })
  }

  def heavyLightParitionAssignment(sc: SparkContext, rddHeavy: RDD[(Int, (Int, Double))], rddLight: RDD[(Int, (Int, Double))],
                                   serverAssignmentHeavyLightMap: Map[Int, (Int, Int)]):
  (RDD[(Int, (Int, (Int, Double)))], RDD[(Int, (Int, (Int, Double)))]) = {
    val rddsHeavyAssigned = new mutable.ListBuffer[RDD[(Int, (Int, (Int, Double)))]]
    val rddsLightAssigned = new mutable.ListBuffer[RDD[(Int, (Int, (Int, Double)))]]

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

  def saltedPartition(rdd: RDD[(Int, (Int, Double))], partitioner: Partitioner): RDD[(Int, (Int, Double))] = {
    val rand = new Random
    rdd.map(row => {(rand.nextInt(partitioner.numPartitions), row)})
      .partitionBy(partitioner).map(_._2)
  }

  def findServerCountHeavyLight(heavyDegreeMap: Map[Int, Long], lightCount: Int, L: Double): mutable.Map[Int, Int] = {
    val HeavyLightServerCount = mutable.Map[Int, Int]()
    heavyDegreeMap.foreach(x => {
      HeavyLightServerCount.put(x._1, math.ceil((x._2 + lightCount) / L).toInt)
    })
    HeavyLightServerCount
  }

  def assignPartNumToLight(sc: SparkContext, k: Int, lightDegreeMap: Map[Int, Long]): Broadcast[Map[Int, Int]] = {
    var serverLightLoads = Vector.fill(k)(0L)
    val lightKeyAssignment: mutable.Map[Int, Int] = mutable.Map()
    lightDegreeMap.toSeq.sortBy(_._2).foreach(
      row => {
        val minLoad = serverLightLoads.min
        val minIndex = serverLightLoads.indexOf(minLoad)
        serverLightLoads = serverLightLoads.updated(minIndex, minLoad + row._2)
        lightKeyAssignment.put(row._1, minIndex)
      }
    )
    sc.broadcast(lightKeyAssignment)
  }

  def dupLightForServers(sc: SparkContext, loc: Int, repeats: Int,
                         lightDegreeMap: Map[Int, Long], rddLight: RDD[(Int, (Int, Double))]): RDD[(Int, (Int, (Int, Double)))] = {
    val lightKeyAssignmentBc = assignPartNumToLight(sc, loc, lightDegreeMap)
    rddLight.flatMap(row => {
      val result = mutable.ListBuffer[(Int, (Int, (Int, Double)))]()
      val keyVal = row._1
      val joinVal = row._2._1
      val weight = row._2._2
      val part = lightKeyAssignmentBc.value(keyVal)
      for (i <- 0 until repeats) {
        result += ((loc * i + part, (keyVal, (joinVal, weight))))
      }
      result
    })
  }


  def main(argv: Array[String]) {
    val args = new WorstCaseOptimalMatrixMultConf(argv)
    logger.info("R1Path: " + args.R1Path())
    logger.info("R2Path: " + args.R2Path())

    val conf = new SparkConf().setAppName("WorstCaseOptimalJoin")
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

    val AHeavyCLightServerCount = findServerCountHeavyLight(R1AHeavyDegree, R2CLightCount, L)
    val ALightCHeavyServerCount = findServerCountHeavyLight(R2CHeavyDegree, R1ALightCount, L)

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
    val notLightLightJoinResultValidated = notLightLightJoinResult.filter(row => row._2._1._2._1 == row._2._2._2._1)

    val matmulNotLightLight = notLightLightJoinResultValidated.map(
      row => ((row._2._1._1, row._2._2._1), row._2._1._2._2 * row._2._2._2._2)
    ).reduceByKey(_+_).map(row => (row._1._1, row._1._2, row._2))
    logger.info("matmulLightLight size: " + matmulNotLightLight.count())

    matmulNotLightLight.sortBy(r => (r._1, r._2, r._3), numPartitions = 1)
      .saveAsTextFile("result/WorstCaseMatMultNotLightLightSorted.txt")

    val k = math.ceil(N1 / L).toInt
    val l = math.ceil(N2 / L).toInt
    logger.info("k size:" + k)
    logger.info("l size:" + l)

    val R1ALightwithPartition = dupLightForServers(sc, k, l, R1ALightDegree, R1ALight)
    val R2CLightwithPartition = dupLightForServers(sc, l, k, R2CLightDegree, R2CLight)

    val lightLightJoinResult = R1ALightwithPartition.join(R2CLightwithPartition)
    val lightLightJoinResultValidated = lightLightJoinResult.filter(row => row._2._1._2._1 == row._2._2._2._1)

    val matmulLightLight = lightLightJoinResultValidated.map(
        row => ((row._2._1._1, row._2._2._1), row._2._1._2._2 * row._2._2._2._2)
      ).reduceByKey(_+_).map(row => (row._1._1, row._1._2, row._2))
    logger.info("matmulLightLight size: " + matmulLightLight.count())

    matmulLightLight.sortBy(r => (r._1, r._2, r._3), numPartitions = 1)
      .saveAsTextFile("result/WorstCaseMatMulLightLightSorted.txt")

    val result = lightLightJoinResultValidated.union(notLightLightJoinResultValidated).coalesce(p).map(
      row => ((row._2._1._1, row._2._2._1), row._2._1._2._2 * row._2._2._2._2)
    ).reduceByKey(_+_).map(row => (row._1._1, row._1._2, row._2))
    result.saveAsTextFile("result/WorstCaseMatMul.txt")

    val resultSorted = result.sortBy(r => (r._1, r._2, r._3), numPartitions = 1)
    resultSorted.saveAsTextFile("result/WorstCaseMatMulSorted.txt")
    logger.info("Total result size: " + resultSorted.count())
  }
}

