name := "ScalaLucene"

version := "0.1"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-core" % "6.0.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "6.0.0",
  "org.apache.lucene" % "lucene-queryparser" % "6.0.0",
  "org.apache.lucene" % "lucene-facet" % "6.0.0",
  "org.apache.lucene" % "lucene-demo" % "6.0.0",
  "org.apache.lucene" % "lucene-highlighter" % "6.0.0",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.jsoup" % "jsoup" % "1.7.2",
  "com.typesafe.play" %% "play-json-joda" % "2.6.+",
  "com.typesafe.akka" %% "akka-stream" % "2.5.6",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.6" % Test
)
        