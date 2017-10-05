package io.saagie.datapipeline.common

case class Configuration(bootstrapServers: String = "localhost:9092", schemaRegistryUrl: String = "http://localhost:8081", topic: String = "test-topic-666")
