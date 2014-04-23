package io.mandelbrot.core.state

import akka.actor.{Props, ActorLogging, Actor}

import org.apache.lucene.store.FSDirectory
import org.apache.lucene.index.{DirectoryReader, Term, IndexWriter, IndexWriterConfig}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.document.Field.Store
import org.apache.lucene.search.{ScoreDoc, IndexSearcher}
import org.apache.lucene.queryparser.classic.{ParseException, QueryParser}
import org.apache.lucene.util.Version
import org.joda.time.DateTime

import io.mandelbrot.core.registry.{ProbeHealth, ProbeLifecycle, ProbeRef}
import io.mandelbrot.core.{ServerConfig, BadRequest, ApiException}

/**
 *
 */
class StateManager extends Actor with ActorLogging {
  import StateManager._

  // config
  val settings = ServerConfig(context.system).settings.state

  /* open the index directory */
  val analyzer = new StandardAnalyzer(LUCENE_VERSION)
  val store = FSDirectory.open(settings.indexDirectory)

  def receive = {

    case update: UpdateProbeStatus =>
      val config = new IndexWriterConfig(LUCENE_VERSION, analyzer)
      val iwriter = new IndexWriter(store, config)
      val doc = new Document()
      doc.add(new StringField("ref", update.probeRef.toString, Store.YES))
      doc.add(new StringField("objectType", "probe", Store.YES))
      doc.add(new LongField("mtime", update.timestamp.getMillis, Store.YES))
      doc.add(new StringField("lifecycle", update.lifecycle.toString, Store.NO))
      doc.add(new StringField("health", update.health.toString, Store.NO))
      iwriter.updateDocument(new Term("ref", update.probeRef.toString), doc)
      iwriter.close()
      // TODO: store the status in history

    case update: UpdateProbeMetadata =>
//      val iwriter = new IndexWriter(metadataStore, config)
//      val doc = new Document()
//      doc.add(new StringField("ref", update.probeRef.toString, Store.YES))
//      update.metadata.foreach { case (name,value) => doc.add(new TextField("meta_" + name, value, Store.NO))}
//      iwriter.updateDocument(new Term("ref", update.probeRef.toString), doc)
//      iwriter.close()

    case query @ QueryProbes(qs, limit) =>
      try {
        val ireader = DirectoryReader.open(store)
        val isearcher = new IndexSearcher(ireader)
        val parser = new QueryParser(LUCENE_VERSION, "ref", analyzer)
        val q = parser.parse(qs)
        val hits: Array[ScoreDoc] = limit match {
          case Some(_limit) =>
            isearcher.search(q, _limit).scoreDocs
          case None =>
            isearcher.search(q, settings.defaultSearchLimit).scoreDocs
        }
        val refs = hits.map {
          hit => ProbeRef(isearcher.doc(hit.doc).get("ref"))
        }.toVector
        ireader.close()
        sender() ! QueryprobesResult(query, refs)
      } catch {
        case ex: ParseException =>
          sender() ! StateServiceOperationFailed(query, new ApiException(BadRequest))
        case ex: Throwable =>
          sender() ! StateServiceOperationFailed(query, ex)
      }
  }
}

object StateManager {
  def props() = Props(classOf[StateManager])

  val LUCENE_VERSION = Version.LUCENE_47
}

case class UpdateProbeStatus(probeRef: ProbeRef, timestamp: DateTime, lifecycle: ProbeLifecycle, health: ProbeHealth, summary: Option[String], detail: Option[String])
case class UpdateProbeMetadata(probeRef: ProbeRef, timestamp: DateTime, metadata: Map[String,String])

/**
 *
 */
sealed trait StateServiceOperation
sealed trait StateServiceCommand extends StateServiceOperation
sealed trait StateServiceQuery extends StateServiceOperation
case class StateServiceOperationFailed(op: StateServiceOperation, failure: Throwable)

case class QueryProbes(query: String, limit: Option[Int]) extends StateServiceQuery
case class QueryprobesResult(op: QueryProbes, refs: Vector[ProbeRef])