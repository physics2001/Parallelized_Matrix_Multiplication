package uwaterloo.mpcmm.matrix

import org.apache.spark.sql.SparkSession

object CreateRDDWithWeightsMoreHeavy {

  def main(args:Array[String]): Unit ={

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("CreateRDDWithWeightsMoreHeavy")
      .getOrCreate()

    val R1 = spark.sparkContext.parallelize(Seq((1, 3, 0.798), (1, 2, 0.991), (1, 4, 0.992), (1, 8, 0.987),
      (1, 7, 0.986), (1, 10, 0.211), (1, 15, 0.175), (1, 16, 0.221), (1, 20, 0.985),
      (2, 7, 0.9090), (2, 1, 0.2), (2, 20, 0.5), (2, 18, 0.433), (2, 6, 0.452), (2, 11, 0.137), (1, 13, 0.157),
      (0, 10, 0.458), (0, 2, 0.296), (0, 0, 0.499), (0, 9, 0.08), (0, 5, 0.718), (0, 3, 0.01), (0, 20, 0.0),
      (0, 19, 0.355), (0, 17, 0.338), (0, 15, 0.032), (0, 18, 0.333), (0, 11, 0.124), (0, 1, 0.37),
      (1, 1, 0.4), (1, 0, 0.66), (4, 8, 0.833), (7, 8, 0.911), (1, 11, 0.488)))
    R1.foreach(println)
    R1.coalesce(1,true).saveAsTextFile("data/R1MoreHeavyWeighted");
    // later, in other program - watch, you have tuples out of the box :)

    val R2 = spark.sparkContext.parallelize(Seq((3, 2, 0.232), (5, 2, 0.314), (8, 2, 0.123), (2, 2, 0.246),
      (1, 2, 0.355), (0, 2, 0.443), (10, 2, 0.361), (6, 2, 0.256), (9, 2, 0.381), (20, 2, 0.47), (18, 2, 0.923),
      (10, 4, 0.508), (6, 4, 0.97), (5, 4, 0.95), (8, 4, 0.92), (9, 4, 0.86), (7, 4, 0.247), (2, 4, 0.01),
      (1, 4, 0.6), (15, 4, 0.17), (19, 4, 0.15), (4, 4, 0.22), (0, 4, 0.08), (3, 4, 0.1),
      (5, 3, 0.511), (2, 3, 0.55), (8, 3, 0.16), (9, 3, 0.04), (17, 3, 0.44), (16, 3, 0.07),
      (6, 9, 0.2), (5, 9, 0.0), (15, 9, 0.9), (16, 9, 0.81), (1, 9, 0.77),
      (18, 0, 0.8), (12, 0, 0.4),
      (6, 6, 0.1), (4, 2, 0.4),
      (11, 2, 0.2)
    ))
    R2.foreach(println)
    R2.coalesce(1,true).saveAsTextFile("data/R2MoreHeavyWeighted")
  }
}
