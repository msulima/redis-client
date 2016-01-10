name := "redis-client-benchmark"

version := "0.1"

scalaVersion := "2.11.5"

libraryDependencies += "redis.clients" % "jedis" % "2.6.2"

libraryDependencies += "io.dropwizard.metrics" % "metrics-core" % "3.1.0"

libraryDependencies += "uk.co.real-logic" % "Agrona" % "0.4.3"

libraryDependencies += "biz.paluch.redis" % "lettuce" % "4.0.Final"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "com.lmax" % "disruptor" % "3.3.2"

javaOptions += "-Xmx2G"
