package io.mandelbrot.persistence.cassandra.dal

import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.{ConsistencyLevel, ResultSet, Session, Statement}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{Future, Promise}

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

case class StatementSettings(consistencyLevel: ConsistencyLevel,
                             serialConsistencyLevel: ConsistencyLevel,
                             fetchSize: Int,
                             retryPolicy: RetryPolicy,
                             tracePercentage: Double)
