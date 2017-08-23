package io.saagie.datapipeline.producer

import com.sksamuel.avro4s.AvroSchema
import io.saagie.datapipeline.common.{Configuration, Request}
import okhttp3.Request.Builder
import okhttp3.{MediaType, OkHttpClient, RequestBody}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.native.JsonMethods._

object RegisterSchema extends App {
  val mediaType = MediaType.parse("application/vnd.schemaregistry.v1+json")
  val client = new OkHttpClient()
  val schema = AvroSchema[Request]
  val jsonSchema: JObject = ("schema" -> schema.toString())
  val configuration = Configuration()

  val request = new Builder()
    .post(RequestBody.create(mediaType, compact(render(jsonSchema)).toString))
    .url(s"${configuration.schemaRegistryUrl}/subjects/Request/versions")
    .build()
  val output = client.newCall(request).execute().body().string()
  println(output)
  println(compact(render(jsonSchema)).toString)
}
