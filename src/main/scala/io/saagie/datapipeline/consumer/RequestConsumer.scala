package io.saagie.datapipeline.consumer

import java.util.Properties

import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import io.saagie.datapipeline.common.{Configuration, Request}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.collection.JavaConverters._

object RequestConsumer extends App {
  val configuration: Configuration = Configuration()
  val properties = new Properties()
  val schema: Schema = AvroSchema[Request]
  val format: RecordFormat[Request] = RecordFormat[Request]
  val injection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

  properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.bootstrapServers)
  properties.put(ConsumerConfig.GROUP_ID_CONFIG, "request-consumer")
  properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)

  val kafkaConsumer = new KafkaConsumer[String, String](properties)

  kafkaConsumer.subscribe(List(configuration.topic).asJava)

  val record = kafkaConsumer.poll(120000)
  println(record.asScala.foreach(r => println(r.value().getBytes.map(_.toChar).mkString)))

//    .map(r => format.from(injection.invert(r.value()).get)).foreach(println))
}
