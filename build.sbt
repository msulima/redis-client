name := "redis-client-benchmark"

version := "0.3.0"

scalaVersion := "2.11.7"

libraryDependencies += "redis.clients" % "jedis" % "2.6.2"

libraryDependencies += "biz.paluch.redis" % "lettuce" % "4.2.2.Final"

libraryDependencies += "io.dropwizard.metrics" % "metrics-core" % "3.1.0"

libraryDependencies += "uk.co.real-logic" % "Agrona" % "0.4.3"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "com.lmax" % "disruptor" % "3.3.2"

libraryDependencies += "org.hdrhistogram" % "HdrHistogram" % "2.1.8"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"

javaOptions += "-Xmx4G"

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
