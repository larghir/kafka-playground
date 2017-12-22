name := "kafka-playground"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "1.0.0",
  "org.apache.spark" %% "spark-parent" % "2.2.1",
  "org.apache.spark" %% "spark-streaming" % "2.2.1",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.1",
  "com.github.benfradet" %% "spark-kafka-writer" % "0.4.0",
  "org.apache.spark" %%"spark-sql" % "2.2.1"
)