package io.saagie.datapipeline.producer

import java.io.File
import java.util.Properties

import akka.actor.{Actor, ActorLogging}
import com.github.tototoshi.csv.CSVWriter
import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import io.saagie.datapipeline.common.{Configuration, Request}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.DefaultFormats

class RequestGenerator extends Actor with ActorLogging {
  val configuration: Configuration = Configuration()
  val properties = new Properties()
  val schema: Schema = AvroSchema[Request]
  val format: RecordFormat[Request] = RecordFormat[Request]
  val injection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)
  var count = 0L
  val start = System.currentTimeMillis()

  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.bootstrapServers)
  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  val kafkaProducer = new KafkaProducer[String, String](properties)

  import org.json4s.native.Serialization.write

  implicit val formats = DefaultFormats

  override def receive: Receive = {
    case "generateSaltRequest" =>
      log.info("Generating salted requests")
      var lines = Seq[Seq[Any]]()
      for (_ <- 0 until 100000) {
        val t = Request.generateSaltRequest().toTuple
        lines = lines :+ Seq(t._1, t._2, t._3, t._4.getOrElse(""), t._5, t._6, t._7, t._8)
      }
      log.info("Requests generated.")
      val writer = CSVWriter.open(new File("dataset3.csv"))
      lines.foreach(writer.writeRow)
      writer.close()
      log.info("Requests writen")
    case "generateRequest" => self ! Request.generateRequest()
    case request: Request =>
      //      val record = format.to(request)
      //      val ba = injection.apply(record)
      log.info("My value {}", write(request))
      kafkaProducer.send(new ProducerRecord(configuration.topic, request.uuid.toString, write(request)))
      count += 1
      if (count % 1000 == 0)
        log.info(s"${count.toDouble / ((System.currentTimeMillis() - start).toDouble / 1000.0)} m/s")
    case x => log.error("Nope {}", x)
  }
}
