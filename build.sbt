name := "redis-client-benchmark"

version := "0.1"

scalaVersion := "2.11.5"

libraryDependencies += "com.typesafe.akka" % "akka-stream-experimental_2.11" % "1.0-M3"

libraryDependencies += "com.typesafe.akka" % "akka-http-experimental_2.11" % "1.0-M3"

libraryDependencies += "com.typesafe.akka" % "akka-http-testkit-experimental_2.11" % "1.0-M3"

libraryDependencies += "com.typesafe.akka" % "akka-http-core-experimental_2.11" % "1.0-M3"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test"
