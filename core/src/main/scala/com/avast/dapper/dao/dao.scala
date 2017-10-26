package com.avast.dapper

import java.util.concurrent.Executor

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{Future, Promise}

package object dao {

  implicit class ListenableFutureToScala[T](val guavaFuture: ListenableFuture[T]) extends AnyVal {
    def asScala(implicit executor: Executor): Future[T] = {
      val p = Promise[T]()

      Futures.addCallback(guavaFuture, new FutureCallback[T] {
        override def onSuccess(result: T): Unit = p.success(result)

        override def onFailure(t: Throwable): Unit = p.failure(t)
      }, executor)

      p.future
    }
  }

}
