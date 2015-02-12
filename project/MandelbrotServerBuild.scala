import sbt._
import Keys._
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import sbtassembly.AssemblyPlugin.assemblySettings

object MandelbrotServerBuild extends Build {

  val mandelbrotVersion = "0.0.8"

  val scalaLangVersion = "2.11.5"
  val scalaParsersVersion = "1.0.3"
  val akkaVersion = "2.3.9"
  val sprayVersion = "1.3.2"
  val sprayJsonVersion = "1.3.1"
  val luceneVersion = "4.7.1"
  val slickVersion = "2.0.3"
  val datastaxVersion = "2.1.4"
  val scalatestVersion = "2.2.4"

  val commonScalacOptions = Seq("-feature", "-deprecation")
  val commonJavacOptions = Seq("-source", "1.7")

  lazy val mandelbrotCoreBuild = (project in file("."))
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(SbtMultiJvm.multiJvmSettings: _*)
    .settings(

      name := "mandelbrot-core",
      version := mandelbrotVersion,

      scalaVersion := scalaLangVersion,
      scalacOptions ++= commonScalacOptions,
      javacOptions ++= commonJavacOptions,
      exportJars := true,

      libraryDependencies ++= Seq(
        "org.scala-lang" % "scala-reflect" % scalaLangVersion,
        "org.scala-lang.modules" %% "scala-parser-combinators" % scalaParsersVersion,
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-remote" % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
        "com.typesafe.akka" %% "akka-contrib" % akkaVersion,
        "io.spray" %% "spray-can" % sprayVersion,
        "io.spray" %% "spray-routing" % sprayVersion,
        "io.spray" %% "spray-json" % sprayJsonVersion,
        "javax.mail" % "mail" % "1.4.7",
        "joda-time" % "joda-time" % "2.2",
        "org.joda" % "joda-convert" % "1.3.1",
        "org.slf4j" % "slf4j-api" % "1.7.5",
        "ch.qos.logback" % "logback-classic" % "1.1.2",
        "org.scalatest" %% "scalatest" % scalatestVersion % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
        "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test",
        "io.spray" %% "spray-testkit" % sprayVersion % "test"
      ),

      // add multi-jvm classes
      unmanagedSourceDirectories in Test += baseDirectory.value / "src" / "multi-jvm" / "scala",

      // make sure that MultiJvm test are compiled by the default test compilation
      //compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
      // disable parallel tests
      parallelExecution in Test := false

//      // make sure that MultiJvm tests are executed by the default test target
//      executeTests in Test <<=
//        (executeTests in Test, executeTests in MultiJvm) map {
//          case ((testResults), (multiJvmResults)) =>
//            val overall =
//              if (testResults.overall.id < multiJvmResults.overall.id)
//                multiJvmResults.overall
//              else
//                testResults.overall
//            Tests.Output(overall,
//              testResults.events ++ multiJvmResults.events,
//              testResults.summaries ++ multiJvmResults.summaries)
//        }

    ).configs(MultiJvm)

  lazy val mandelbrotServerCassandraBuild = (project in file("persistence-cassandra"))
    .settings(assemblySettings: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(

      name := "mandelbrot-server-cassandra",
      version := mandelbrotVersion,

      scalaVersion := scalaLangVersion,
      scalacOptions ++= commonScalacOptions,
      javacOptions ++= commonJavacOptions,
      exportJars := true,

      libraryDependencies ++= Seq(
        "com.datastax.cassandra" % "cassandra-driver-core" % datastaxVersion,
        "org.scalatest" %% "scalatest" % scalatestVersion % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
      )

  ).dependsOn(mandelbrotCoreBuild)

  lazy val mandelbrotServerSlickBuild = (project in file("persistence-slick"))
    .settings(assemblySettings: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(

      name := "mandelbrot-server-slick",
      version := mandelbrotVersion,

      scalaVersion := scalaLangVersion,
      scalacOptions ++= commonScalacOptions,
      javacOptions ++= commonJavacOptions,
      exportJars := true,

      libraryDependencies ++= Seq(
        "com.typesafe.slick" %% "slick" % slickVersion,
        "com.h2database" % "h2" % "1.4.177",
        "org.scalatest" %% "scalatest" % scalatestVersion % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
      )

  ).dependsOn(mandelbrotCoreBuild)

}
