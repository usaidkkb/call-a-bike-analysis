name := "call-a-bike-analysis"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "2.4.5",
  "org.apache.spark" % "spark-sql_2.12" % "2.4.5"
)