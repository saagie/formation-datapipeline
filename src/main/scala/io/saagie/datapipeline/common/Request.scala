package io.saagie.datapipeline.common

import java.time.Instant
import java.util.UUID

import fabricator.{Fabricator, Internet, Location, UserAgent}
import org.joda.time.DateTimeZone

import scala.collection.mutable
import scala.util.Random

case class Request(uuid: UUID, origin: String, ip: String, userAgent: Option[String], timestamp: Long, latitude: Double, longitude: Double) {
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

  def apply(): Request = new Request(UUID.randomUUID(), "", "", None, 0L, 0, 0)

  val userAgent: UserAgent = Fabricator.userAgent()
  val internet: Internet = Fabricator.internet()
  val location: Location = Fabricator.location()
  val columnsNames = Seq("uuid", "origin", "ip", "user_agent", "timestamp", "latitude", "longitude")

  def generateRequest(): Request = {
    new Request(UUID.randomUUID(),
      internet.urlBuilder.host("formation.test").params(mutable.Map("q" -> s"${Random.nextInt(32)}")).toString,
      internet.ip,
      if (Random.nextBoolean) Some(userAgent.browser) else None,
      Instant.now.getEpochSecond,
      location.latitude(-89, 89).toDouble,
      location.longitude(-179, 179).toDouble)

    location.coordinates
  }
}
