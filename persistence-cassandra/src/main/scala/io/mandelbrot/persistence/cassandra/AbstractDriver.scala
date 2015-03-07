package io.mandelbrot.persistence.cassandra

import com.datastax.driver.core.{ResultSet, Statement, Session}
import com.google.common.util.concurrent.{Futures, FutureCallback, ListenableFuture}
import scala.concurrent.{Promise, Future}

/**
 *
 */
trait AbstractDriver {

  val session: Session

  private def listenable2scalaFuture[T](f: ListenableFuture[T]): Future[T] = {
    val p = Promise[T]()
    Futures.addCallback(f, new FutureCallback[T] {
        def onSuccess(r: T) = p success r
        def onFailure(t: Throwable) = p failure t
    })
    p.future
  }

  def executeAsync(statement: Statement): Future[ResultSet] = {
    listenable2scalaFuture(session.executeAsync(statement))
  }

  def executeAsync(query: String): Future[ResultSet] = {
    listenable2scalaFuture(session.executeAsync(query))
  }
}
