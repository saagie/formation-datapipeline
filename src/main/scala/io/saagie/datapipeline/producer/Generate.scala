package io.saagie.datapipeline.producer

import akka.actor.{ActorSystem, Props}
import com.sksamuel.avro4s.AvroSchema
import io.saagie.datapipeline.common.Request

object Generate extends App {
  val schema = AvroSchema[Request]
  val system = ActorSystem("Generate")
  val generator = system.actorOf(Props[RequestGenerator])
  val begin = System.currentTimeMillis()
//  generator ! "generateSaltRequest"

  while (true) {
    //  for (_ <- 0 until 1000000) {
    generator ! "generateRequest"
    Thread.sleep(5000)
    println("One request sent")
  }
  println(s"${System.currentTimeMillis() - begin}ms")
}
