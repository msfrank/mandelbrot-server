import sbt._
import Keys._
import com.github.retronym.SbtOneJar

object MandelbrotServerBuild extends Build {

  val mandelbrotVersion = "0.0.4"

  val scalaLangVersion = "2.10.4"
  val akkaVersion = "2.3.3"
  val sprayVersion = "1.3.1"
  val luceneVersion = "4.7.1"
  val esperVersion = "4.11.0"
  val slickVersion = "2.0.1"

  lazy val mandelbrotBuild = Project(
    id = "mandelbrot-server",
    base = file("."),
    settings = Project.defaultSettings ++ SbtOneJar.oneJarSettings ++ Seq(

      exportJars := true,
      name := "mandelbrot-server",
      version := mandelbrotVersion,
      scalaVersion := scalaLangVersion,
      javacOptions ++= Seq("-source", "1.7"),

      libraryDependencies ++= Seq(
        "org.scala-lang" % "scala-reflect" % scalaLangVersion,
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
        "com.typesafe.slick" %% "slick" % slickVersion,
        "com.h2database" % "h2" % "1.4.177",
        "javax.mail" % "mail" % "1.4.7",
        "joda-time" % "joda-time" % "2.2",
        "org.joda" % "joda-convert" % "1.3.1",
        "org.slf4j" % "slf4j-api" % "1.7.5",
        "ch.qos.logback" % "logback-classic" % "1.1.2",
        "org.scalatest" %% "scalatest" % "1.9.1" % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
        "io.spray" % "spray-testkit" % sprayVersion % "test"
      ),

      fork in (Test,run) := true  // for akka-persistence leveldb plugin
    )
  )
}
