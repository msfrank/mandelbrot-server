import sbt._
import Keys._
import sbtassembly.Plugin.assemblySettings
import sbtassembly.Plugin.AssemblyKeys._

object MandelbrotServerBuild extends Build {

  val mandelbrotVersion = "0.0.8"
  val scalaLangVersion = "2.10.4"
  val akkaVersion = "2.3.6"
  val sprayVersion = "1.3.1"
  val luceneVersion = "4.7.1"
  val slickVersion = "2.0.3"
  val datastaxVersion = "2.1.2"

  lazy val mandelbrotCoreBuild = (project in file("."))
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(

      exportJars := true,
      name := "mandelbrot-core",
      version := mandelbrotVersion,
      scalaVersion := scalaLangVersion,
      scalacOptions ++= Seq("-feature", "-deprecation"),
      javacOptions ++= Seq("-source", "1.7"),

      libraryDependencies ++= Seq(
        "org.scala-lang" % "scala-reflect" % scalaLangVersion,
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-remote" % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
        "com.typesafe.akka" %% "akka-contrib" % akkaVersion,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
        "io.spray" % "spray-can" % sprayVersion,
        "io.spray" % "spray-routing" % sprayVersion,
        "io.spray" %% "spray-json" % "1.2.5",
        "org.apache.lucene" % "lucene-core" % luceneVersion,
        "org.apache.lucene" % "lucene-analyzers-common" % luceneVersion,
        "org.apache.lucene" % "lucene-memory" % luceneVersion,
        "org.apache.lucene" % "lucene-queryparser" % luceneVersion,
        "javax.mail" % "mail" % "1.4.7",
        "joda-time" % "joda-time" % "2.2",
        "org.joda" % "joda-convert" % "1.3.1",
        "org.slf4j" % "slf4j-api" % "1.7.5",
        "ch.qos.logback" % "logback-classic" % "1.1.2",
        "org.scalatest" %% "scalatest" % "1.9.1" % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
        "io.spray" % "spray-testkit" % sprayVersion % "test"
      )
  )

  lazy val mandelbrotServerCassandraBuild = (project in file("persistence-cassandra"))
    .settings(assemblySettings: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(

      exportJars := true,
      name := "mandelbrot-server-cassandra",
      version := mandelbrotVersion,
      scalaVersion := scalaLangVersion,
      scalacOptions ++= Seq("-feature", "-deprecation"),
      javacOptions ++= Seq("-source", "1.7"),

      resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven",

      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-persistence-experimental" % akkaVersion exclude("org.google.guava", "guava"),
        "com.github.krasserm" %% "akka-persistence-cassandra" % "0.3.4"
        //"com.datastax.cassandra" % "cassandra-driver-core" % datastaxVersion
      ),

      fork in (Test,run) := true  // for akka-persistence leveldb plugin

  ).dependsOn(mandelbrotCoreBuild)

  lazy val mandelbrotServerSlickBuild = (project in file("persistence-slick"))
    .settings(assemblySettings: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(

      exportJars := true,
      name := "mandelbrot-server-slick",
      version := mandelbrotVersion,
      scalaVersion := scalaLangVersion,
      scalacOptions ++= Seq("-feature", "-deprecation"),
      javacOptions ++= Seq("-source", "1.7"),

      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-persistence-experimental" % akkaVersion,
        "com.typesafe.slick" %% "slick" % slickVersion,
        "com.h2database" % "h2" % "1.4.177"
      )

  ).dependsOn(mandelbrotCoreBuild)

}
