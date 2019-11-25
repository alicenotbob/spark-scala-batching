name := "spark-batching"
version := "0.1"
scalaVersion := "2.11.12"
parallelExecution in Test := false
val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "com.holdenkarau" % "spark-testing-base_2.11" % "2.4.3_0.12.0",
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "com.databricks" %% "spark-avro" % "4.0.0"
)