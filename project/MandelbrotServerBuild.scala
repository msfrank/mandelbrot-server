import sbt._
import Keys._
import com.github.retronym.SbtOneJar

object MandelbrotCommon {
  val mandelbrotVersion = "0.0.8"
  val scalaLangVersion = "2.10.4"
  val akkaVersion = "2.3.6"
  val sprayVersion = "1.3.1"
  val luceneVersion = "4.7.1"
  val slickVersion = "2.0.3"
  val datastaxVersion = "2.1.2"
}

object MandelbrotServerBuild extends Build {
  import MandelbrotCommon._

  lazy val mandelbrotServerBuild = (project in file(".")).settings(

      exportJars := true,
      name := "mandelbrot-server",
      version := mandelbrotVersion,
      scalaVersion := scalaLangVersion,
      scalacOptions ++= Seq("-feature", "-deprecation"),
      javacOptions ++= Seq("-source", "1.7"),

      libraryDependencies ++= Seq(
        "org.scala-lang" % "scala-reflect" % scalaLangVersion,
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-persistence-experimental" % akkaVersion,
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
      ),

      fork in (Test,run) := true  // for akka-persistence leveldb plugin
  ).settings(SbtOneJar.oneJarSettings:_*)

  lazy val mandelbrotPersistenceCassandraBuild = (project in file("persistence-cassandra")).settings(

    exportJars := true,
    name := "mandelbrot-persistence-cassandra",
    version := mandelbrotVersion,
    scalaVersion := scalaLangVersion,
    scalacOptions ++= Seq("-feature", "-deprecation"),
    javacOptions ++= Seq("-source", "1.7"),

    libraryDependencies ++= Seq(
      "com.datastax.cassandra" % "cassandra-driver-core" % datastaxVersion,
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
    )
  ).settings(SbtOneJar.oneJarSettings:_*).dependsOn(mandelbrotServerBuild)

  lazy val mandelbrotPersistenceSlickBuild = (project in file("persistence-slick")).settings(

    exportJars := true,
    name := "mandelbrot-persistence-slick",
    version := mandelbrotVersion,
    scalaVersion := scalaLangVersion,
    scalacOptions ++= Seq("-feature", "-deprecation"),
    javacOptions ++= Seq("-source", "1.7"),

    libraryDependencies ++= Seq(
      "com.typesafe.slick" %% "slick" % slickVersion,
      "com.h2database" % "h2" % "1.4.177",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
    )
  ).settings(SbtOneJar.oneJarSettings:_*).dependsOn(mandelbrotServerBuild)

}
