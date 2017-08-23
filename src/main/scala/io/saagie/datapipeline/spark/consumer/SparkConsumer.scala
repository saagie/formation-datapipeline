package io.saagie.datapipeline.spark.consumer

import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.saagie.datapipeline.common.{Configuration, Request}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkConsumer extends App {
  val configuration: Configuration = Configuration()
  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Datapipeline Formation")
  val streamingContext = new StreamingContext(sparkConf, Seconds(300))

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> configuration.bootstrapServers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[KafkaAvroDeserializer],
    "group.id" -> "request-consumer",
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean),
    "schema.registry.url" -> configuration.schemaRegistryUrl
  )

  val format = RecordFormat[Request]
  val topics = Array(configuration.topic)
  val stream = KafkaUtils.createDirectStream[String, GenericRecord](
    streamingContext,
    PreferConsistent,
    Subscribe[String, GenericRecord](topics, kafkaParams)
  )

  streamingContext.getState()

  stream
    .map(r => format.from(r.value()))
    .foreachRDD(rdd => {
      rdd
        .groupBy(_.userAgent)
        .map(r => (r._1, r._2.size))
        .reduceByKey(_ + _)
        .foreach(println)
    })

  streamingContext.start()
  streamingContext.awaitTermination()
}
