package com.avast

import java.util.concurrent.Executor

import com.datastax.driver.core.Session
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{Future, Promise}
import scala.language.experimental.macros

package object dapper {

  private[dapper] implicit class ListenableFutureToScala[T](val guavaFuture: ListenableFuture[T]) extends AnyVal {
    def asScala(implicit executor: Executor): Future[T] = {
      val p = Promise[T]()

      Futures.addCallback(guavaFuture, new FutureCallback[T] {
        override def onSuccess(result: T): Unit = p.success(result)

        override def onFailure(t: Throwable): Unit = p.failure(t)
      }, executor)

      p.future
    }
  }

  implicit class DatastaxSession(val session: Session) extends AnyVal {
    def createDaoFor[PrimaryKey, Entity <: CassandraEntity[PrimaryKey]]:  CassandraDao[PrimaryKey, Entity] =
      macro Macros.createDao[PrimaryKey, Entity]
  }

}
