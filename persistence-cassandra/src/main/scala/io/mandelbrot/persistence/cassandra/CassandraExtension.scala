package io.mandelbrot.persistence.cassandra

import akka.actor._
import com.datastax.driver.core.{Session, Cluster}
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

import io.mandelbrot.core.ServerConfig

/**
 *
 */
class CassandraExtension(system: ActorSystem) extends Extension {

  private val log = LoggerFactory.getLogger(classOf[CassandraExtension])
  private val config = system.settings.config.getConfig("mandelbrot.persistence.cassandra")

  val seedNodes = config.getStringList("seed-nodes")
  val replicationFactor = config.getInt("replication-factor")
  val keyspaceName = config.getString("keyspace-name")

  /* create the cluster */
  private var cluster = Cluster.builder().addContactPoints(seedNodes:_*).build()
  log.debug("using seed nodes {}", seedNodes.mkString(", "))

  private var session: Session = null

  /**
   * connect to the cassandra cluster, create the keyspace if necessary, set the session
   * to use the keyspace, then return the configured session.  this method should be used
   * instead of the individual methods below, which are exposed purely for testing.
   */
  def getSession: Session = synchronized {
    connect()
    createAndUseKeyspace()
    session
  }

  /**
   * connect to the cluster.  DON'T USE THIS.  use getSession() instead.
   */
  def connect() = {
    if (session == null) {
      log.debug("connecting to cluster: {}", cluster.getMetadata.getClusterName)
      session = cluster.connect()
    }
  }

  /**
   * create the keyspace if it doesn't exist, then set the session to use the keyspace.
   * DON'T USE THIS.  use getSession() instead.
   */
  def createAndUseKeyspace() = {
    session.execute(
      s"""
         |CREATE KEYSPACE IF NOT EXISTS $keyspaceName
         |WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : $replicationFactor }
      """.stripMargin)
    log.debug("using keyspace {}", keyspaceName)
    session.execute("USE " + keyspaceName)
  }

  /**
   * drop the keyspace if it exists.  DON'T USE THIS.  use getSession() instead.
   */
  def dropKeyspace() = {
    session.execute(
      s"""
         |DROP KEYSPACE IF EXISTS $keyspaceName
      """.stripMargin)
  }

}

/**
 *
 */
object Cassandra extends ExtensionId[CassandraExtension] with ExtensionIdProvider {
  override def lookup() = Cassandra
  override def createExtension(system: ExtendedActorSystem) = new CassandraExtension(system)
}
