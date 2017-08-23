package io.saagie.datapipeline.producer

import java.util.Properties

import akka.actor.{Actor, ActorLogging}
import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import io.saagie.datapipeline.common.{Configuration, Request}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

class RequestGenerator extends Actor with ActorLogging {
  val configuration: Configuration = Configuration()
  val properties = new Properties()
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.bootstrapServers)
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getName)
  properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, configuration.schemaRegistryUrl)
  val kafkaProducer = new KafkaProducer[String, GenericRecord](properties)

  override def receive: Receive = {
    case "generateRequest" => self ! Request.generateRequest()
    case request: Request => {
      val format = RecordFormat[Request]
      val record = format.to(request)
      kafkaProducer.send(new ProducerRecord(configuration.topic, request.uuid.toString, record))
      log.info("sent")
    }
    case x => log.error("Nope {}", x)
  }
}
