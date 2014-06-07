/**
 * Copyright 2014 Michael Frank <msfrank@syntaxjockey.com>
 *
 * This file is part of Mandelbrot.
 *
 * Mandelbrot is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Mandelbrot is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Mandelbrot.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.mandelbrot.core.state

import com.typesafe.config.Config
import akka.actor.{Props, ActorLogging, Actor}
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.index.{DirectoryReader, Term, IndexWriter, IndexWriterConfig}
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document._
import org.apache.lucene.document.Field.Store
import org.apache.lucene.search.{ScoreDoc, IndexSearcher}
import org.apache.lucene.queryparser.classic.{ParseException, QueryParser}
import org.apache.lucene.util.Version
import scala.util.{Failure, Success}
import java.io.File

import io.mandelbrot.core.registry._
import io.mandelbrot.core.{ServerConfig, BadRequest, ApiException}
import io.mandelbrot.core.state.LuceneSearcher.ManagerSettings

/**
 *
 */
class LuceneSearcher(managerSettings: ManagerSettings) extends Actor with ActorLogging {
  import LuceneSearcher._

  // config
  val settings = ServerConfig(context.system).settings.state

  /* open the index directory */
  val analyzer = new StandardAnalyzer(LUCENE_VERSION)
  val store = FSDirectory.open(managerSettings.indexDirectory)

  def receive = {

    case status: ProbeStatus =>
      val config = new IndexWriterConfig(LUCENE_VERSION, analyzer)
      val iwriter = new IndexWriter(store, config)
      val doc = new Document()
      doc.add(new StringField("id", "status:" + status.probeRef.toString, Store.YES))
      doc.add(new StringField("ref", status.probeRef.toString, Store.YES))
      doc.add(new StringField("lifecycle", status.lifecycle.toString, Store.NO))
      doc.add(new StringField("health", status.health.toString, Store.NO))
      for (lastChange <- status.lastChange)
        doc.add(new LongField("changed", lastChange.getMillis, Store.NO))
      for (lastUpdate <- status.lastUpdate)
        doc.add(new LongField("updated", lastUpdate.getMillis, Store.NO))
      for (correlation <- status.correlation)
        doc.add(new StringField("correlation", correlation.toString, Store.NO))
      for (acknowledged <- status.acknowledged)
        doc.add(new StringField("acknowledged", acknowledged.toString, Store.NO))
      iwriter.updateDocument(new Term("id", "status:" + status.probeRef.toString), doc)
      iwriter.close()

    case metadata: ProbeMetadata =>
      val config = new IndexWriterConfig(LUCENE_VERSION, analyzer)
      val iwriter = new IndexWriter(store, config)
      val doc = new Document()
      doc.add(new StringField("id", "meta:" + metadata.probeRef.toString, Store.YES))
      doc.add(new StringField("ref", metadata.probeRef.toString, Store.YES))
      metadata.metadata.foreach { case (name,value) => doc.add(new TextField("meta_" + name, value, Store.NO))}
      iwriter.updateDocument(new Term("id", "meta:" + metadata.probeRef.toString), doc)
      iwriter.close()

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
        sender() ! Success(ProbeResults(refs))
      } catch {
        case ex: ParseException =>
          sender() ! Failure(new ApiException(BadRequest))
        case ex: Throwable =>
          sender() ! Failure(ex)
      }
  }
}

object LuceneSearcher {
  def props(managerSettings: ManagerSettings) = Props(classOf[LuceneSearcher], managerSettings)

  case class ManagerSettings(indexDirectory: File)
  def settings(config: Config): Option[ManagerSettings] = {
    val indexDirectory = new File(config.getString("index-directory"))
    Some(ManagerSettings(indexDirectory))
  }

  val LUCENE_VERSION = Version.LUCENE_47

  case class DeleteProbeState(probeRef: ProbeRef)
}

