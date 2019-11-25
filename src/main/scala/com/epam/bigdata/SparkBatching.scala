package com.epam.bigdata

import com.epam.bigdata.util.ConnectionConfig.KAFKA_CONNECTION_CONFIG
import org.apache.spark.sql.functions.{datediff, from_json, last, row_number, year, count}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Encoders, Row, SaveMode, SparkSession}
import com.epam.bigdata.util.SchemaHolder.{EXPEDIA_SCHEMA, HOTELS_FORECAST_SCHEMA}
import org.apache.spark.sql.expressions.Window

object SparkBatching {

  private val HDFS_ROOT = sys.env.getOrElse("HDFS_ROOT", "hdfs://sandbox-hdp.hortonworks.com:8020/")
  private val EXPEDIA_PATH = sys.env.getOrElse("EXPEDIA_PATH", "user/uladzislau/expedia/")

  val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-batching")
      .getOrCreate()

  import spark.implicits._

  def readHotelsAndWeatherData: Dataset[HotelForecast] = {
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

  def readExpediaData: DataFrame = spark.read.format("com.databricks.spark.avro").schema(EXPEDIA_SCHEMA).load(HDFS_ROOT + EXPEDIA_PATH)

  def calculateIdleDaysForEveryHotel(expedia: DataFrame): DataFrame = {
    val id = expedia("id")
    val hotelId = expedia("hotel_id")
    val checkIn = expedia("srch_ci")
    expedia
      .select(id, hotelId, checkIn, last(checkIn) over Window.partitionBy(hotelId).orderBy(checkIn).rowsBetween(Window.unboundedPreceding, -1) as "prev_srch_ci")
      .select(id, hotelId, checkIn, $"prev_srch_ci", datediff(checkIn, $"prev_srch_ci") as "idle_days")
  }

  def getExpediaForHotelsWithValidDataOnly(expediaWithIdleDays: DataFrame): DataFrame = {
    expediaWithIdleDays select expediaWithIdleDays("*") join (
      (expediaWithIdleDays select $"hotel_id" where $"idle_days" >= 2 && $"idle_days" < 30) distinct,
      Seq("hotel_id"),
      "leftanti"
    )
  }

  def getInvalidHotelsData(expediaWithIdleDays: DataFrame, hotelsWeather: Dataset[HotelForecast]) = {
    val invalidHotelIds = (expediaWithIdleDays select $"hotel_id" where $"idle_days" >= 2 && $"idle_days" < 30) distinct

    hotelsWeather
      .select($"hotel.*")
      .distinct()
      .join(invalidHotelIds, $"id" === $"hotel_id", "leftsemi")
  }

  def getTheRemainingDataAndBookingCounts(validExpedia: DataFrame, hotelForecast: Dataset[HotelForecast]): DataFrame = {
    hotelForecast
      .select($"hotel")
      .distinct()
      .join(validExpedia, $"hotel.id" === $"hotel_id")
      .groupBy($"hotel.country", $"hotel.city")
      .count()
  }

  def storeValidExpediaInHdfs(validExpedia: DataFrame): Unit = {
    validExpedia
      .select($"*", year($"srch_ci") as "check_in_year")
      .write
      .format("com.databricks.spark.avro")
      .mode(SaveMode.Overwrite)
      .partitionBy("check_in_year")
      .save(sys.env.getOrElse("RESULT_PATH", HDFS_ROOT + EXPEDIA_PATH + "../spark_batching_valid_expedia/"))
  }

  def main(args: Array[String]): Unit = {
    val hotelsWeather = readHotelsAndWeatherData
    val expedia = readExpediaData
    val expediaWithIdleDays = calculateIdleDaysForEveryHotel(expedia)
    val validExpedia = getExpediaForHotelsWithValidDataOnly(expediaWithIdleDays)
    getInvalidHotelsData(expediaWithIdleDays, hotelsWeather).show(false)
    getTheRemainingDataAndBookingCounts(validExpedia, hotelsWeather).show(false)
    storeValidExpediaInHdfs(validExpedia)
    spark.stop
  }
}
