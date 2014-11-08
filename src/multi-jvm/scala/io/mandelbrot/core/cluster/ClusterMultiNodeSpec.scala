package io.mandelbrot.core.cluster

import com.typesafe.config.ConfigFactory
import akka.remote.testkit._
import akka.util.Timeout
import org.scalatest.{WordSpecLike, WordSpec, BeforeAndAfterAll}
import org.scalatest.matchers.MustMatchers
import scala.concurrent.duration._

import org.slf4j.LoggerFactory
import ch.qos.logback.classic.{LoggerContext, Level}
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.FileAppender
import ch.qos.logback.classic.spi.ILoggingEvent

abstract class ClusterMultiNodeSpec(config: MultiNodeConfig) extends MultiNodeSpec(config)
  with MultiNodeSpecCallbacks
  with WordSpecLike
  with MustMatchers
  with BeforeAndAfterAll {

  override def beforeAll() = multiNodeSpecBeforeAll()
  override def afterAll() = multiNodeSpecAfterAll()

  implicit val timeout = Timeout(10.seconds)

  private val lc = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]

  private val ple = new PatternLayoutEncoder()
  ple.setPattern("%X{akkaTimestamp} %-5level %X{akkaSource} - %msg%n")
  ple.setContext(lc)
  ple.start()

  private val fileAppender = new FileAppender[ILoggingEvent]()
  fileAppender.setFile("target/test_%s.log".format(getClass.getSimpleName))
  fileAppender.setEncoder(ple)
  fileAppender.setContext(lc)
  fileAppender.start()

  private val rootLogger = lc.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)
  rootLogger.setLevel(Level.INFO)
  rootLogger.addAppender(fileAppender)
  private val smrLogger = lc.getLogger("io.mandelbrot.core.cluster")
  smrLogger.setLevel(Level.DEBUG)
  smrLogger.setAdditive(false)
  smrLogger.addAppender(fileAppender)
}

object ClusterMultiNodeConfig extends MultiNodeConfig {
  commonConfig(ConfigFactory.load("multi-jvm.conf"))
  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")
  val node4 = role("node4")
  val node5 = role("node5")
}
