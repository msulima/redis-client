name := "redis-client-benchmark"

version := "0.3.0"

scalaVersion := "2.11.7"

libraryDependencies += "redis.clients" % "jedis" % "2.6.2"

libraryDependencies += "biz.paluch.redis" % "lettuce" % "4.2.2.Final"

libraryDependencies += "io.dropwizard.metrics" % "metrics-core" % "3.1.0"

libraryDependencies += "org.agrona" % "agrona" % "0.9.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "com.lmax" % "disruptor" % "3.3.6"

libraryDependencies += "org.hdrhistogram" % "HdrHistogram" % "2.1.9"

libraryDependencies += "junit" % "junit" % "4.12"

libraryDependencies += "org.assertj" % "assertj-core" % "3.6.2"

javaOptions ++= Seq("-Xmx4G", "-Xms4G", "-XX:+UseConcMarkSweepGC", "-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder")

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
