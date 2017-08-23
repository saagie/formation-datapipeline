package io.saagie.datapipeline.common

case class Configuration(bootstrapServers: String = "192.168.61.50:31200", schemaRegistryUrl: String = "http://localhost:8081", topic: String = "test-topic-666")
