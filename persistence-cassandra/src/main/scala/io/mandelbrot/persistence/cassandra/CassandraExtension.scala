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

  def getSession: Session = synchronized {
    if (session == null) {
      /* connect to the cluster */
      session = cluster.connect()
      log.debug("connected to cluster: {}", cluster.getMetadata.getClusterName)
      /* create the keyspace if it doesn't exist */
      session.execute(
        s"""
           |CREATE KEYSPACE IF NOT EXISTS $keyspaceName
           |WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : $replicationFactor }
        """.stripMargin)
      /* configure session to use the keyspace for subsequent operations */
      log.debug("using keyspace {}", keyspaceName)
      session.execute("USE " + keyspaceName)
    }
    session
  }
}

/**
 *
 */
object Cassandra extends ExtensionId[CassandraExtension] with ExtensionIdProvider {
  override def lookup() = Cassandra
  override def createExtension(system: ExtendedActorSystem) = new CassandraExtension(system)
}
