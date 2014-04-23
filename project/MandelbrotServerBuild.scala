import sbt._
import Keys._

object MandelbrotServerBuild extends Build {

  val mandelbrotVersion = "0.1"

  val akkaVersion = "2.3.2"
  val sprayVersion = "1.3.1"
  val luceneVersion = "4.7.1"
  val esperVersion = "4.11.0"
  val slickVersion = "2.0.1"

  lazy val mandelbrotBuild = Project(
    id = "mandelbrot-server",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      exportJars := true,
      name := "mandelbrot-server",
      version := mandelbrotVersion,
      scalaVersion := "2.10.4",
      javacOptions ++= Seq("-source", "1.7"),
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-persistence-experimental" % akkaVersion,
        "com.typesafe.akka" %% "akka-remote" % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
        "com.typesafe.akka" %% "akka-zeromq" % akkaVersion,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
        "io.spray" % "spray-can" % sprayVersion,
        "io.spray" % "spray-routing" % sprayVersion,
        "io.spray" %% "spray-json" % "1.2.5",
        "org.apache.lucene" % "lucene-core" % luceneVersion,
        "org.apache.lucene" % "lucene-analyzers-common" % luceneVersion,
        "org.apache.lucene" % "lucene-memory" % luceneVersion,
        "org.apache.lucene" % "lucene-queryparser" % luceneVersion,
        "com.espertech" % "esper" % esperVersion,
        "com.typesafe.slick" %% "slick" % slickVersion,
        "com.escalatesoft.subcut" %% "subcut" % "2.0",
        "joda-time" % "joda-time" % "2.2",
        "org.joda" % "joda-convert" % "1.3.1",
        "nl.grons" %% "metrics-scala" % "3.0.5_a2.3",
        "org.slf4j" % "slf4j-api" % "1.7.5",
        "org.slf4j" % "slf4j-log4j12" % "1.7.5",
        "org.scalatest" %% "scalatest" % "1.9.1" % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
      ),
      javaOptions in test += "-Dlog4j.configuration=src/test/resources/log4j.properties"
    )
  )
}
