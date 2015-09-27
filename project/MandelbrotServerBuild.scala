import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import sbt._
import Keys._
import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm
import sbtassembly.AssemblyPlugin.assemblySettings
import sbtassembly.AssemblyKeys._
import sbtassembly.PathList
import sbtassembly.MergeStrategy
import com.typesafe.sbt.SbtNativePackager.autoImport._

object MandelbrotServerBuild extends Build {

  val mandelbrotVersion = "0.0.10"

  val scalaLangVersion = "2.11.7"
  val scalaParsersVersion = "1.0.3"
  val akkaVersion = "2.3.14"
  val sprayVersion = "1.3.3"
  val sprayJsonVersion = "1.3.1"
  val slickVersion = "2.0.3"
  val datastaxVersion = "2.1.7"
  val jodaTimeVersion = "2.8.2"
  val jodaConvertVersion = "1.7"
  val scalatestVersion = "2.2.5"

  val mandelbrotOrganization = "io.mandelbrot"
  val mandelbrotHomepage = new URL("https://github.com/msfrank/mandelbrot-server")

  val commonScalacOptions = Seq("-feature", "-deprecation")
  val commonJavacOptions = Seq("-source", "1.8")

  /**
   *
   */
  lazy val coreModel = (project in file("core-model"))
    .settings(

      name := "mandelbrot-core-model",
      version := mandelbrotVersion,
      organization := mandelbrotOrganization,
      organizationHomepage := Some(mandelbrotHomepage),

      scalaVersion := scalaLangVersion,
      scalacOptions ++= commonScalacOptions,
      javacOptions ++= commonJavacOptions,
      exportJars := true,

      libraryDependencies ++= Seq(
        "joda-time" % "joda-time" % jodaTimeVersion,
        "org.joda" % "joda-convert" % jodaConvertVersion,
        "org.scalatest" %% "scalatest" % scalatestVersion % "test"
      ),

      // enable parallel tests
      parallelExecution in Test := true,

      // publish in the Maven style, with a pom.xml
      publishMavenStyle := true
  )

  /**
   *
   */
  lazy val coreServer = (project in file("."))
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(

      name := "mandelbrot-core-server",
      version := mandelbrotVersion,
      organization := mandelbrotOrganization,
      organizationHomepage := Some(mandelbrotHomepage),

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
        "io.spray" %% "spray-testkit" % sprayVersion % "test"
      ),

      // disable parallel tests
      parallelExecution in Test := false,

      // don't run tests when building assembly jar
      test in assembly := {},

      // publish in the Maven style, with a pom.xml
      publishMavenStyle := true

    ).dependsOn(coreModel % "compile->compile;test->test")

  /**
   *
   */
  lazy val cassandraServer = (project in file("persistence-cassandra"))
    .enablePlugins(JavaAppPackaging)
    .settings(assemblySettings: _*)
    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
    .settings(

      name := "mandelbrot-server-cassandra",
      version := mandelbrotVersion,
      organization := mandelbrotOrganization,
      organizationHomepage := Some(mandelbrotHomepage),

      scalaVersion := scalaLangVersion,
      scalacOptions ++= commonScalacOptions,
      javacOptions ++= commonJavacOptions,
      exportJars := true,

      libraryDependencies ++= Seq(
        "com.datastax.cassandra" % "cassandra-driver-core" % datastaxVersion,
        "org.cassandraunit" % "cassandra-unit" % "2.0.2.2" % "test",
        "org.scalatest" %% "scalatest" % scalatestVersion % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
      ),

      // specify the main class to use
      mainClass in assembly := Some("io.mandelbrot.persistence.cassandra.CassandraApplication"),

      // disable parallel tests
      parallelExecution in Test := false,

      // don't run tests when building assembly jar
      test in assembly := {},

      assemblyMergeStrategy in assembly := {
        // concatenate service loader files
        case PathList("META-INF", "services", xs @_*) => MergeStrategy.concat
        // discard any files in META-INF/maven/
        case PathList("META-INF", "maven", xs @_*) => MergeStrategy.discard
        // use the default for anything else
        case otherwise => (assemblyMergeStrategy in assembly).value(otherwise)
      },

      // publish in the Maven style, with a pom.xml
      publishMavenStyle := true

  ).dependsOn(coreServer % "compile->compile;test->test")

//  lazy val slickServer = (project in file("persistence-slick"))
//    .enablePlugins(JavaAppPackaging)
//    .settings(assemblySettings: _*)
//    .settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
//    .settings(
//
//      name := "mandelbrot-server-slick",
//      version := mandelbrotVersion,
//
//      scalaVersion := scalaLangVersion,
//      scalacOptions ++= commonScalacOptions,
//      javacOptions ++= commonJavacOptions,
//      exportJars := true,
//
//      libraryDependencies ++= Seq(
//        //"com.typesafe.slick" %% "slick" % slickVersion,
//        "com.h2database" % "h2" % "1.4.177",
//        "org.scalatest" %% "scalatest" % scalatestVersion % "test",
//        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
//      ),
//
//      // disable parallel tests
//      parallelExecution in Test := false,
//
//      // don't run tests when building assembly jar
//      test in assembly := {}
//
//  ).dependsOn(coreServer)

  lazy val integrationTests = (project in file("integration-tests"))
    .settings(SbtMultiJvm.multiJvmSettings: _*)
    .settings(

      name := "integration-tests",
      version := mandelbrotVersion,
      organization := mandelbrotOrganization,
      organizationHomepage := Some(mandelbrotHomepage),

      scalaVersion := scalaLangVersion,
      scalacOptions ++= commonScalacOptions,
      javacOptions ++= commonJavacOptions,
      exportJars := false,

      libraryDependencies ++= Seq(
        "org.scalatest" %% "scalatest" % scalatestVersion % "test",
        "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
      ),

      // add multi-jvm classes
      unmanagedSourceDirectories in Test += baseDirectory.value / "src" / "multi-jvm" / "scala",

      // make sure that MultiJvm test are compiled by the default test compilation
      compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),

      // disable parallel tests
      parallelExecution in Test := false,

      // make sure that MultiJvm tests are executed by the default test target
      executeTests in Test <<=
        (executeTests in Test, executeTests in MultiJvm) map {
          case ((testResults), (multiJvmResults)) =>
            val overall =
              if (testResults.overall.id < multiJvmResults.overall.id)
                multiJvmResults.overall
              else
                testResults.overall
            Tests.Output(overall,
              testResults.events ++ multiJvmResults.events,
              testResults.summaries ++ multiJvmResults.summaries)
        }

  ).dependsOn(coreServer % "compile->compile;test->test")
   .configs(MultiJvm)

}
