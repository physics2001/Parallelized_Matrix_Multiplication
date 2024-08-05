package uwaterloo.mpcmm.utils

import org.rogach.scallop.ScallopConf

class AppConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(R1Path,R2Path)
  val R1Path = opt[String](descr = "R1 path", required = true)
  val R2Path = opt[String](descr = "R2 path", required = true)
  val numReducers = opt[Int](descr = "number of reducers", required = true)
  val resultFolder = opt[String](descr = "folder for results", required = true)
  val verbose = opt[Boolean](descr = "verbose", required=true)
  verify()
}