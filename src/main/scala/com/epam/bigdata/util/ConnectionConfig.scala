package com.epam.bigdata.util

object ConnectionConfig {
    val KAFKA_CONNECTION_CONFIG: Map[String, String] = Map(
      "kafka.bootstrap.servers" -> "sandbox-hdp.hortonworks.com:6667",
      "subscribe" -> "hotels_forecast_formatted_2",
      "startingOffsets" -> """{"hotels_forecast_formatted_2":{"0":0}}""",
      "endingOffsets" -> """{"hotels_forecast_formatted_2":{"0":63813003}}"""
    )
}
