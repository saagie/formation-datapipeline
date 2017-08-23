package io.saagie.datapipeline.common

import java.time.Instant
import java.util.UUID

import fabricator.{Fabricator, Internet, UserAgent}
import org.joda.time.DateTimeZone

import scala.collection.mutable
import scala.util.Random

case class Request(uuid: UUID, origin: String, ip: String, userAgent: Option[String], timestamp: Long) {
  /**
    * Trick to transform UUID as String.
    * Used as a workaround Spark's serialization.
    *
    * @return A beautiful tuple
    */
  def toTuple: (String, String, String, Option[String], Long) = (uuid.toString, origin, ip, userAgent, timestamp)
}

object Request {
  DateTimeZone.setDefault(DateTimeZone.UTC)

  def apply(): Request = new Request(UUID.randomUUID(), "", "", None, 0L)

  val userAgent: UserAgent = Fabricator.userAgent()
  val internet: Internet = Fabricator.internet()
  val columnsNames = Seq("uuid", "origin", "ip", "user_agent", "timestamp")

  def generateRequest(): Request = {
    new Request(UUID.randomUUID(),
      internet.urlBuilder.host("formation.test").params(mutable.Map("q" -> s"${Random.nextInt(32)}")).toString,
      internet.ip,
      if (Random.nextBoolean) Some(userAgent.browser) else None,
      Instant.now.getEpochSecond)
  }
}
