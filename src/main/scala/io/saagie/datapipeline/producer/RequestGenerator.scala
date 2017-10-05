package io.saagie.datapipeline.producer

import java.util.Properties

import akka.actor.{Actor, ActorLogging}
import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import io.saagie.datapipeline.common.{Configuration, Request}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

class RequestGenerator extends Actor with ActorLogging {
  val configuration: Configuration = Configuration()
  val properties = new Properties()
  val schema: Schema = AvroSchema[Request]
  val format: RecordFormat[Request] = RecordFormat[Request]
  val injection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.bootstrapServers)
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
  val kafkaProducer = new KafkaProducer[String, Array[Byte]](properties)

  override def receive: Receive = {
    case "generateRequest" => self ! Request.generateRequest()
    case request: Request =>
      val record = format.to(request)
      val ba = injection.apply(record)
      kafkaProducer.send(new ProducerRecord(configuration.topic, request.uuid.toString, ba))
      log.info("sent")
    case x => log.error("Nope {}", x)
  }
}
