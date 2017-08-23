package io.saagie.datapipeline.producer

import akka.actor.{ActorSystem, Props}
import com.sksamuel.avro4s.AvroSchema
import io.saagie.datapipeline.common.Request

object Generate extends App {
  val schema = AvroSchema[Request]
  val system = ActorSystem("Generate")
  val generator = system.actorOf(Props[RequestGenerator])
  val begin = System.currentTimeMillis()
  while (true) {
    generator ! "generateRequest"
    Thread.sleep(1000)
  }
  println(s"${System.currentTimeMillis() - begin}ms")
}
