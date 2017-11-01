package com.avast.dapper

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Milliseconds, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.util.Random

abstract class TestBase extends FunSuite with ScalaFutures with BeforeAndAfterAll with BeforeAndAfterEach with Eventually {
  protected val config: Config = ConfigFactory.load().getConfig("test")

  protected implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(5, Seconds), interval = Span(200, Milliseconds))

  def randomString(length: Int): String = {
    Random.alphanumeric.take(length).mkString("")
  }
}
