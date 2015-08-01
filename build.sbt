name := "redis-client-benchmark"

version := "0.1"

scalaVersion := "2.11.5"

enablePlugins(GatlingPlugin)

resolvers ++= Seq(
  "brando" at "http://chrisdinn.github.io/releases/"
)

libraryDependencies ++= Seq(
  "io.spray" %% "spray-can" % "1.3.3",
  "io.spray" %% "spray-routing" % "1.3.3"
)

libraryDependencies += "io.netty" % "netty-all" % "4.0.30.Final"

libraryDependencies += "commons-io" % "commons-io" % "2.4"

libraryDependencies += "redis.clients" % "jedis" % "2.6.2"

libraryDependencies += "com.digital-achiever" %% "brando" % "2.0.6"

libraryDependencies += "org.apache.avro" % "avro" % "1.7.7"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"

libraryDependencies += "io.gatling.highcharts" % "gatling-charts-highcharts" % "2.1.3" % "test"

libraryDependencies += "io.gatling" % "gatling-test-framework" % "2.1.3" % "test"

javaOptions += "-Xmx2G"
