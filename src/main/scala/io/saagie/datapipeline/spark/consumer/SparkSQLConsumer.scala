package io.saagie.datapipeline.spark.consumer

import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.saagie.datapipeline.common.{Configuration, Request}
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLConsumer extends App {
  val configuration: Configuration = Configuration()
  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Datapipeline SQL Formation")
    .getOrCreate()

  val format = RecordFormat[Request]
  val schema = AvroSchema[Request]

  import sparkSession.implicits._

  val topic = Seq(configuration.topic)
  val dataset: DataFrame = sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", configuration.bootstrapServers)
    .option("startingOffsets", "earliest")
    .option("subscribe", configuration.topic)
    .load()

  val schemaRegistryClient = new CachedSchemaRegistryClient(configuration.schemaRegistryUrl, 1000)
  val deserializer = new KafkaAvroDeserializer(schemaRegistryClient)

  dataset
    .select($"value".as[Array[Byte]])
    .map(v => format.from(deserializer.deserialize("", v, schema).asInstanceOf[GenericRecord]).toTuple)
    .toDF(Request.columnsNames: _*)
    .groupBy($"user_agent")
    .count()
    .orderBy($"count" desc)
    .writeStream
    .format("console")
    .outputMode(OutputMode.Complete())
    .option("truncate", value = false)
    .start()
    .awaitTermination()
}
