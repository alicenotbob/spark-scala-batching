package com.epam.bigdata.util

object ConnectionConfig {

  def KAFKA_CONNECTION_CONFIG: Map[String, String] = {
    val bootstrapServer: String = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVER", "sandbox-hdp.hortonworks.com:6667")
    val kafkaTopic: String = sys.env.getOrElse("KAFKA_TOPIC", "hotels_forecast")
    val lastIndex: String = sys.env.getOrElse("LAST_INDEX", "63813003")
    Map(
      "kafka.bootstrap.servers" -> bootstrapServer,
      "subscribe" -> kafkaTopic,
      "startingOffsets" -> s"""{"$kafkaTopic":{"0":0}}""",
      "endingOffsets" -> s"""{"$kafkaTopic":{"0":$lastIndex}}"""
    )
  }
}