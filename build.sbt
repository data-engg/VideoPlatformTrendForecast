name := "Mid project 2"

description := ""

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.12"