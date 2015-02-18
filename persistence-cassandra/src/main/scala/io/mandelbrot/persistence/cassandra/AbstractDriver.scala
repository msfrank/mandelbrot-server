package io.mandelbrot.persistence.cassandra

import com.datastax.driver.core.Session
import com.google.common.util.concurrent.{Futures, FutureCallback, ListenableFuture}
import scala.concurrent.{ExecutionContext, Promise, Future}

/**
 *
 */
abstract class AbstractDriver(val session: Session, ec: ExecutionContext) {
  import scala.language.implicitConversions

  implicit def guavaFutureToAkka[T](f: ListenableFuture[T]): Future[T] = {
    val p = Promise[T]()
    Futures.addCallback(f,
      new FutureCallback[T] {
        def onSuccess(r: T) = p success r
        def onFailure(t: Throwable) = p failure t
      })
    p.future
  }
}
