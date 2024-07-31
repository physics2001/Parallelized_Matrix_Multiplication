ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

assemblyJarName in assembly := "algorithm.jar"

lazy val root = (project in file("."))
  .settings(
    name := "Parallelized_Matrix_Multiplication"
  )

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-core" % "2.19.0" % Runtime,
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.rogach" %% "scallop" % "5.1.0",
  "org.apache.hadoop" % "hadoop-common" % "3.3.6"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
