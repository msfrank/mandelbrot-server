package io.mandelbrot.core.entity

import akka.remote.testkit._
import akka.util.Timeout
import ch.qos.logback.classic.{Level, LoggerContext}
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.FileAppender
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}
import org.scalatest.ShouldMatchers
import org.slf4j.LoggerFactory
import scala.concurrent.duration._
import java.io.File

abstract class MultiNodeSpec(config: MultiNodeConfig) extends akka.remote.testkit.MultiNodeSpec(config)
  with MultiNodeSpecCallbacks
  with WordSpecLike
  with ShouldMatchers
  with BeforeAndAfterAll {

  override def beforeAll() = multiNodeSpecBeforeAll()
  override def afterAll() = multiNodeSpecAfterAll()

  implicit val timeout = Timeout(10.seconds)

  private val lc = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]

  private val ple = new PatternLayoutEncoder()
  ple.setPattern("%X{akkaTimestamp} %-5level %X{akkaSource} - %msg%n")
  ple.setContext(lc)
  ple.start()

  private val fileAppender = {
    val file = new File("target/test_%s.log".format(getClass.getSimpleName))
    if (file.exists()) {
      file.delete()
      println("removed log file " + file.getAbsolutePath)
    }
    val appender = new FileAppender[ILoggingEvent]()
    appender.setFile(file.getAbsolutePath)
    appender.setEncoder(ple)
    appender.setContext(lc)
    appender.start()
    appender
  }

  private val rootLogger = lc.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
  rootLogger.setLevel(Level.INFO)
  rootLogger.addAppender(fileAppender)
  private val mandelbrotLogger = lc.getLogger("io.mandelbrot")
  mandelbrotLogger.setLevel(Level.DEBUG)
  mandelbrotLogger.setAdditive(false)
  mandelbrotLogger.addAppender(fileAppender)
}

object RemoteMultiNodeConfig extends MultiNodeConfig {
  commonConfig(ConfigFactory.load("multi-jvm-remote.conf"))
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  val node4 = role("node4")
  val node5 = role("node5")
}

object ClusterMultiNodeConfig extends MultiNodeConfig {
  commonConfig(ConfigFactory.load("multi-jvm-cluster.conf"))
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  val node4 = role("node4")
  val node5 = role("node5")
}
