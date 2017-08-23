package io.saagie.datapipeline.consumer

import java.util.Properties

import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer}
import io.saagie.datapipeline.common.{Configuration, Request}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

object RequestConsumer extends App {
  val format = RecordFormat[Request]
  val configuration: Configuration = Configuration()
  val properties = new Properties()
  properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.bootstrapServers)
  properties.put(ConsumerConfig.GROUP_ID_CONFIG, "request-consumer")
  properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getName)
  properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, configuration.schemaRegistryUrl)

  val kafkaConsumer = new KafkaConsumer[String, GenericRecord](properties)

  kafkaConsumer.subscribe(List(configuration.topic).asJava)

  val record = kafkaConsumer.poll(10000)
  println(record.asScala.map(r => format.from(r.value())).foreach(println))
}
