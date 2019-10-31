package com.epam.bigdata

import com.epam.bigdata.util.ConnectionConfig.KAFKA_CONNECTION_CONFIG
import org.apache.spark.sql.functions.{datediff, from_json, last, row_number, year}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders, Row, SaveMode, SparkSession}
import com.epam.bigdata.util.SchemaHolder.{EXPEDIA_SCHEMA, HOTELS_FORECAST_SCHEMA}
import org.apache.spark.sql.expressions.Window

object HelloSpark {

  private final val HDFS_ROOT = "hdfs://localhost:8020/"
  private final val EXPEDIA_PATH = "user/uladzislau/expedia/"

  def getSparkSession: SparkSession = {
    SparkSession
      .builder()
      .appName("HelloSpark")
      .master("local[*]")
      .getOrCreate()
  }

  def readHotelsAndWeatherData(spark: SparkSession): Dataset[HotelForecast] = {
    import spark.implicits._
    val df = spark
      .read
      .format("kafka")
      .options(KAFKA_CONNECTION_CONFIG)
      .load
    df.select(from_json(df.col("value").cast("string"), HOTELS_FORECAST_SCHEMA).as("data"))
      .select($"data.*", row_number over Window.partitionBy($"data.hotel.id", $"data.weather.year", $"data.weather.month", $"data.weather.day").orderBy($"data.precision" desc) as "rownum")
      .where($"rownum" === 1)
      .select($"precision", $"weather", $"hotel")
      .as[HotelForecast](Encoders.product[HotelForecast])
  }

  def readExpediaData(sparkSession: SparkSession): DataFrame = sparkSession.read.format("avro").schema(EXPEDIA_SCHEMA).load(HDFS_ROOT + EXPEDIA_PATH)

  def calculateIdleDaysForEveryHotel(spark: SparkSession, expedia: DataFrame): DataFrame = {
    import spark.implicits._
    val hotelId = expedia("hotel_id")
    val checkIn = expedia("srch_ci")
    expedia
      .select(hotelId, checkIn, last(checkIn) over Window.partitionBy(hotelId).orderBy(checkIn).rowsBetween(Window.unboundedPreceding, -1) as "prev_srch_ci")
      .select(hotelId, checkIn, $"prev_srch_ci", datediff(checkIn, $"prev_srch_ci") as "idle_days")
  }

  def getExpediaOnlyForHotelsWithValidData(spark: SparkSession, expediaWithIdleDays: DataFrame): DataFrame = {
    import spark.implicits._
    expediaWithIdleDays select expediaWithIdleDays("*") join (
      (expediaWithIdleDays select $"hotel_id" where $"idle_days" >= 2 && $"idle_days" < 30) distinct,
      Seq("hotel_id"),
      "leftanti"
    )
  }

  def getInvalidHotelsData(spark: SparkSession,
                           expediaWithIdleDays: DataFrame,
                           validExpedia: DataFrame,
                           hotelsWeather: Dataset[HotelForecast]): Dataset[Hotel] = {
    import spark.implicits._
    val invalidHotelIds = expediaWithIdleDays
      .select($"hotel_id")
      .join(validExpedia select $"hotel_id" distinct, Seq("hotel_id"), "leftanti")
      .distinct()
    hotelsWeather.select($"hotel.*")
      .join(invalidHotelIds, $"id" === invalidHotelIds("hotel_id"), "leftsemi")
      .as[Hotel]
  }

  def groupTheRemainingDataAndPrintBookingCounts(spark: SparkSession, expedia: DataFrame) = {
    import spark.implicits._
    expedia.groupBy($"")
  }

  def storeValidExpediaInHdfs(spark: SparkSession, validExpedia: DataFrame) = {
    import spark.implicits._
    validExpedia
      .select($"*", year($"srch_ci") as "check_in_year")
      .write
      .format("avro")
      .mode(SaveMode.Overwrite)
      .partitionBy("check_in_year")
      .save(HDFS_ROOT + EXPEDIA_PATH + "../spark_batching_valid_expedia/")
  }

  def main(args: Array[String]): Unit = {
    val spark = getSparkSession
    val hotelsWeather = readHotelsAndWeatherData(spark)
    val expedia = readExpediaData(spark)
    val expediaWithIdleDays = calculateIdleDaysForEveryHotel(spark, expedia)
    val validExpedia = getExpediaOnlyForHotelsWithValidData(spark, expediaWithIdleDays)
    val invalidHotels = getInvalidHotelsData(spark, expediaWithIdleDays, validExpedia, hotelsWeather)
    invalidHotels.show(false)
//    groupTheRemainingDataAndPrintBookingCounts(spark, expedia)
    storeValidExpediaInHdfs(spark, validExpedia)
    spark.stop
  }
}
