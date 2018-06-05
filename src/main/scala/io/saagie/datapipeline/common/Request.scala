package io.saagie.datapipeline.common

import java.time.Instant
import java.util.UUID

import fabricator._
import org.joda.time.DateTimeZone

import scala.collection.mutable
import scala.util.Random
import scala.util.matching.Regex

case class Request(uuid: String, origin: String, ip: String, userAgent: Option[String], timestamp: Long, latitude: Double, longitude: Double, price: Double) {
  /**
    * Trick to transform UUID as String.
    * Used as a workaround Spark's serialization.
    *
    * @return A beautiful tuple
    */
  def toTuple: (String, String, String, Option[String], Long, Double, Double, Double) = (uuid.toString, origin, ip, userAgent, timestamp, latitude, longitude, price)
}

object Request {
  DateTimeZone.setDefault(DateTimeZone.UTC)

  def apply(): Request = new Request(UUID.randomUUID().toString, "", "", None, 0L, 0, 0, 0)

  val columnsNames = Seq("uuid", "origin", "ip", "user_agent", "timestamp", "latitude", "longitude", "price")
  val userAgent: UserAgent = Fabricator.userAgent()
  val internet: Internet = Fabricator.internet()
  val location: Location = Fabricator.location()
  val words: Words = Fabricator.words()

  def price: Double = Random.nextDouble() * Random.nextInt(40)

  def price(max:Int): Double = Random.nextDouble() * Random.nextInt(max)

  def price(range: Range): Double = (Random.nextDouble() * Random.nextInt(range.max)) + range.min

  val opera: Regex = ".*Opera.*".r
  val chrome: Regex = ".*Chrome.*".r
  val firefox: Regex = ".*Firefox.*".r

  def latitudeSalt(browser: Option[String]): Double = {
    browser match {
      case Some(b) => b match {
        case opera() => location.latitude(-45, 50).toDouble
        case chrome() => location.latitude(-20, 10).toDouble
        case firefox() => location.latitude(-20, 70).toDouble
        case _ => location.latitude(-89, 89).toDouble
      }
      case None => location.latitude(-89, 89).toDouble
    }
  }

  def nav(ua: String): String = {
    ua match {
      case opera() => "Opera"
      case chrome() => "Chrome"
      case firefox() => "Firefox"
      case _ => ua
    }
  }

  def priceSalt(browser: Option[String]): Double = {
    browser match {
      case Some(b) => b match {
        case opera() => price(270 to 370)
        case chrome() => price(180 to 280)
        case firefox() => price(90 to 190)
        case _ => price(0 to 100)
      }
      case None => price(0 to 100)
    }
  }

  def generateRequest(): Request = {
    new Request(UUID.randomUUID().toString,
      internet.urlBuilder.host("formation.test").params(mutable.Map("q" -> s"${Random.nextInt(32)}")).toString,
      internet.ip,
      if (Random.nextBoolean) Some(userAgent.browser) else None,
      Instant.now.getEpochSecond,
      location.latitude(-89, 89).toDouble,
      location.longitude(-179, 179).toDouble,
      price
    )
  }

  def generateSaltRequest(): Request = {
    val ua = if (Random.nextBoolean) Some(userAgent.browser) else None
    val price = priceSalt(ua)
    var word = ""
    while (word.isEmpty) {
      word = words.word
    }
    val latitude = latitudeSalt(ua)
    val url = internet.urlBuilder.host("formation.test").params(mutable.Map("q" -> word)).toString
    Request(UUID.randomUUID().toString,
      url,
      internet.ip,
      ua.map(nav),
      Instant.now.getEpochSecond,
      latitude,
      location.longitude(-179, 179).toDouble,
      price
    )
  }
}
