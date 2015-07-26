name := "redis-client-benchmark"

version := "0.1"

scalaVersion := "2.11.5"

enablePlugins(GatlingPlugin)

resolvers ++= Seq(
  "brando" at "http://chrisdinn.github.io/releases/"
)

libraryDependencies += "com.typesafe.akka" %% "akka-stream-experimental" % "1.0"

libraryDependencies += "com.typesafe.akka" %% "akka-http-experimental" % "1.0"

libraryDependencies += "com.typesafe.akka" %% "akka-http-testkit-experimental" % "1.0"

libraryDependencies += "com.typesafe.akka" %% "akka-http-core-experimental" % "1.0"

libraryDependencies += "io.netty" % "netty-all" % "4.0.30.Final"

libraryDependencies += "redis.clients" % "jedis" % "2.6.2"

libraryDependencies += "com.digital-achiever" %% "brando" % "2.0.6"

libraryDependencies += "org.apache.avro" % "avro" % "1.7.7"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "io.gatling.highcharts" % "gatling-charts-highcharts" % "2.1.3" % "test"

libraryDependencies += "io.gatling" % "gatling-test-framework" % "2.1.3" % "test"

javaOptions += "-Xmx2G"
