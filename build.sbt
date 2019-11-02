name := "spark-batching"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.holdenkarau" % "spark-testing-base_2.11" % "2.4.3_0.12.0",
  "org.apache.spark" % "spark-core_2.11" % "2.4.4",
  "org.apache.spark" % "spark-sql_2.11" % "2.4.4",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.4.4",
  "org.apache.spark" % "spark-avro_2.11" % "2.4.4"
)

parallelExecution in Test := false