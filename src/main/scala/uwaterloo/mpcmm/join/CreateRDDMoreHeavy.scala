package uwaterloo.mpcmm.join

import org.apache.spark.sql.SparkSession

object CreateRDDMoreHeavy {

  def main(args:Array[String]): Unit ={

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("CreateRDDMoreHeavy")
      .getOrCreate()

    val R1 = spark.sparkContext.parallelize(Seq((1, 3), (1, 2), (1, 4), (1, 8), (1, 7), (1, 10), (1, 15), (1, 16), (1, 20),
      (2, 7), (2, 1), (2, 20), (2, 18), (2, 6), (2, 11), (1, 13),
      (0, 10), (0, 2), (0, 0), (0, 9), (0, 5), (0, 3), (0, 20), (0, 19), (0, 17), (0, 15), (0, 18), (0, 11), (0, 1),
      (1, 1), (1, 0), (4, 8), (7, 8), (1, 11)))
    R1.foreach(println)
    R1.coalesce(1,true).saveAsTextFile("data/R1MoreHeavy");
    // later, in other program - watch, you have tuples out of the box :)

    val R2 = spark.sparkContext.parallelize(Seq((3, 2), (5, 2), (8, 2), (2, 2), (1, 2), (0, 2), (10, 2), (6, 2), (9, 2), (20, 2), (18, 2),
      (10, 4), (6, 4), (5, 4), (8, 4), (9, 4), (7, 4), (2, 4), (1, 4), (15, 4), (19, 4), (4, 4), (0, 4), (3, 4),
      (5, 3), (2, 3), (8, 3), (9, 3), (17, 3), (16, 3),
      (6, 9), (5, 9), (15, 9), (16, 9), (1, 9),
      (18, 0), (12, 0),
      (6, 6), (4, 2),
      (11, 2)
    ))
    R2.foreach(println)
    R2.coalesce(1,true).saveAsTextFile("data/R2MoreHeavy")
  }
}
