package io.saagie.datapipeline.spark.consumer

import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import io.saagie.datapipeline.common.{Configuration, Request}
import org.apache.avro.Schema
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

  val schema: Schema = AvroSchema[Request]
  val format: RecordFormat[Request] = RecordFormat[Request]
  val injection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)

  import sparkSession.implicits._

  val topic = Seq(configuration.topic)
  val dataset: DataFrame = sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", configuration.bootstrapServers)
    .option("startingOffsets", "earliest")
    .option("subscribe", configuration.topic)
    .load()

  dataset
    .select($"value".as[Array[Byte]])
    .map(v => format.from(injection.invert(v).get).toTuple)
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
