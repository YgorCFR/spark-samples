name := "Simple Project"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.13.3"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.13.3"