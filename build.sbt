name := "misc-kafka"

version := "0.1"

scalaVersion := "2.12.7"

resolvers += Resolver.mavenLocal

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.12"
libraryDependencies +="com.typesafe.akka" %% "akka-http"   % "10.1.5"
libraryDependencies +="com.typesafe.akka" %% "akka-stream" % "2.5.12"
libraryDependencies += "com.typesafe.akka" %% "akka-http-testkit" % "10.1.5"
libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.5"
libraryDependencies += "io.spray" %%  "spray-json" % "1.3.5"
libraryDependencies += "org.mockito" % "mockito-core" % "2.10.0" % Test
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.0.0"
libraryDependencies += "net.manub" %% "scalatest-embedded-kafka" % "2.0.0" % Test
libraryDependencies += "javax.ws.rs" % "javax.ws.rs-api" % "2.1" artifacts( Artifact("javax.ws.rs-api", "jar", "jar"))
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.0.1"
libraryDependencies += "io.confluent.ksql" % "ksql-udf" % "5.0.0"





