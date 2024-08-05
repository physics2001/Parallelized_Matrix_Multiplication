ThisBuild / version := "1.0.0"

ThisBuild / scalaVersion := "2.13.14"

assemblyJarName in assembly := s"algorithm-${version.value}.jar"

lazy val root = (project in file("."))
  .settings(
    name := "Parallelized_Matrix_Multiplication"
  )

libraryDependencies ++= Seq(
  "org.apache.logging.log4j" % "log4j-core" % "2.19.0" % Runtime,
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.rogach" %% "scallop" % "5.1.0",
  "org.apache.commons" % "commons-math3" % "3.6.1",
  "org.apache.hadoop" % "hadoop-common" % "3.3.6"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
