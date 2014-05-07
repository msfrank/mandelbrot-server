import sbt._
import Keys._
import com.typesafe.sbt.SbtProguard

object MandelbrotServerBuild extends Build {

  val mandelbrotVersion = "0.0.1"

  val scalaLangVersion = "2.10.4"
  val akkaVersion = "2.3.2"
  val sprayVersion = "1.3.1"
  val luceneVersion = "4.7.1"
  val esperVersion = "4.11.0"
  val slickVersion = "2.0.1"

  lazy val mandelbrotBuild = Project(
    id = "mandelbrot-server",
    base = file("."),
    settings = Project.defaultSettings ++ SbtProguard.proguardSettings ++ Seq(

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
        "joda-time" % "joda-time" % "2.2",
        "org.joda" % "joda-convert" % "1.3.1",
        "org.slf4j" % "slf4j-api" % "1.7.5",
        "ch.qos.logback" % "logback-classic" % "1.1.2",
        "org.scalatest" %% "scalatest" % "1.9.1" % "test",
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
        "io.spray" % "spray-testkit" % sprayVersion % "test"
      ),
      fork in (Test,run) := true // for akka-persistence leveldb plugin
    ) ++ MandelbrotProguardOptions.proguardSettings
  )
}

object MandelbrotProguardOptions {

  import com.typesafe.sbt.SbtProguard._
  import ProguardKeys.{options,proguard}
  import ProguardOptions.keepMain

  lazy val proguardSettings: Seq[Setting[_]] = inConfig(Proguard)(Seq(
      javaOptions in proguard := Seq(
        "-Xmx8192M",
        "-XX:+UseConcMarkSweepGC",
        "-XX:+CMSClassUnloadingEnabled",
        "-XX:-UseGCOverheadLimit",
        "-XX:PermSize=512M",
        "-XX:MaxPermSize=2048M",
        "-XX:NewSize=512M",
        "-XX:MaxNewSize=2048M"
        ),
      options in Proguard += keepMain("io.mandelbrot.core.MandelbrotApp$"),
      options in Proguard ++= Seq("-dontshrink", "-dontoptimize", "-dontobfuscate", "-verbose"),
      options in Proguard +=
        """
          |#
          |# MANDELBROT
          |#
          |
          |-keep class io.mandelbrot.** {
          |      *;
          |}
          |
          |#
          |# SCALA
          |#
          |
          |-keepclasseswithmembers public class * {
          |    public static void main(java.lang.String[]);
          |}
          |
          |-keepclassmembers class * { ** MODULE$; }
          |
          |-keepclassmembernames class scala.concurrent.forkjoin.ForkJoinPool {
          |      long ctl;
          |}
          |
          |-keepclassmembernames class scala.concurrent.forkjoin.ForkJoinPool$WorkQueue {
          |      int runState;
          |}
          |
          |-keepclassmembernames class scala.concurrent.forkjoin.LinkedTransferQueue {
          |      scala.concurrent.forkjoin.LinkedTransferQueue$Node head;
          |        scala.concurrent.forkjoin.LinkedTransferQueue$Node tail;
          |          int sweepVotes;
          |}
          |
          |-keepclassmembernames class scala.concurrent.forkjoin.LinkedTransferQueue$Node {
          |      java.lang.Object item;
          |        scala.concurrent.forkjoin.LinkedTransferQueue$Node next;
          |          java.lang.Thread waiter;
          |}
          |
          |-dontnote scala.xml.**
          |-dontnote scala.concurrent.forkjoin.ForkJoinPool
          |-dontwarn scala.**
          |
          |#
          |# AKKA
          |#
          |
          |-keep class akka.actor.** {
          |      *;
          |}
          |
          |-keepclassmembernames class * implements akka.actor.Actor {
          |      akka.actor.ActorContext context;
          |        akka.actor.ActorRef self;
          |}
          |
          |-keep class * implements akka.actor.ActorRefProvider {
          |      public <init>(...);
          |}
          |
          |-keep class * implements akka.actor.ExtensionId {
          |      public <init>(...);
          |}
          |
          |-keep class * implements akka.actor.ExtensionIdProvider {
          |      public <init>(...);
          |}
          |
          |-keep class akka.actor.SerializedActorRef {
          |      *;
          |}
          |
          |-keep class * implements akka.actor.SupervisorStrategyConfigurator {
          |      public <init>(...);
          |}
          |
          |-keep class * extends akka.dispatch.ExecutorServiceConfigurator {
          |      public <init>(...);
          |}
          |
          |-keep class * implements akka.dispatch.MailboxType {
          |      public <init>(...);
          |}
          |
          |-keep class * extends akka.dispatch.MessageDispatcherConfigurator {
          |      public <init>(...);
          |}
          |
          |-keep class akka.event.Logging*
          |
          |-keep class akka.event.Logging$LogExt {
          |      public <init>(...);
          |}
          |
          |-keep class akka.remote.DaemonMsgCreate {
          |      *;
          |}
          |
          |-keep class * extends akka.remote.RemoteTransport {
          |      public <init>(...);
          |}
          |
          |-keep class * implements akka.routing.RouterConfig {
          |      public <init>(...);
          |}
          |
          |-keep class * implements akka.serialization.Serializer {
          |      public <init>(...);
          |}
          |
          |-dontwarn akka.remote.netty.NettySSLSupport**
          |-dontnote akka.**
          |
          |
          |#
          |# SPRAY
          |#
          |
          |-keep class spray.** {
          |      *;
          |}
          |
          |
          |#
          |# H2
          |#
          |
          |-dontwarn org.h2.server.web.**
          |
          |
          |#
          |# LOGBACK
          |#
          |
          |-keep class ch.qos.logback.** {
          |      *;
          |}
          |
          |
          |#
          |# PROTOBUF
          |#
          |
          |-keep class * extends com.google.protobuf.GeneratedMessage {
          |      ** newBuilder();
          |}
          |
          |
          |#
          |# NETTY
          |#
          |
          |-keep class * implements org.jboss.netty.channel.ChannelHandler
          |
          |-dontnote org.jboss.netty.util.internal.**
          |-dontwarn org.jboss.netty.**
          |
          |#
          |# LUCENE
          |#
          |
          |-keep class org.apache.lucene.** {
          |    *;
          |}
          |-keep class org.tartarus.snowball.** {
          |    *;
          |}
        """.stripMargin
  ))
}
