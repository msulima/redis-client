name := "redis-client-benchmark"

version := "0.1"

scalaVersion := "2.11.5"

resolvers ++= Seq(
  "brando" at "http://chrisdinn.github.io/releases/"
)

libraryDependencies += "com.typesafe.akka" %% "akka-stream-experimental" % "1.0-M3"

libraryDependencies += "com.typesafe.akka" %% "akka-http-experimental" % "1.0-M3"

libraryDependencies += "com.typesafe.akka" %% "akka-http-testkit-experimental" % "1.0-M3"

libraryDependencies += "com.typesafe.akka" %% "akka-http-core-experimental" % "1.0-M3"

libraryDependencies += "redis.clients" % "jedis" % "2.6.2"

libraryDependencies += "com.digital-achiever" %% "brando" % "2.0.6"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.1" % "test"
