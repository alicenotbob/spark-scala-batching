package com.epam.bigdata

import com.epam.bigdata.SparkBatching.{calculateIdleDaysForEveryHotel, getExpediaForHotelsWithValidDataOnly, getInvalidHotelsData, getTheRemainingDataAndBookingCounts}
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, DatasetSuiteBase, RDDComparisons}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class Test extends FunSuite with DataFrameSuiteBase with RDDComparisons with DatasetSuiteBase {

  import sqlContext.implicits._

  def testExpediaData: DataFrame = {
    val cols = Seq("id", "srch_ci", "hotel_id")
    Seq(
      (1L, "2016-05-27", 1L),
      (2L, "2016-04-28", 1L),
      (3L, "2016-03-28", 1L),
      (4L, "2017-01-14", 2L),
      (5L, "2017-02-27", 2L),
      (6L, "2017-11-13", 2L),
      (7L, "2018-01-01", 3L),
      (8L, "2018-01-01", 3L),
      (9L, "2017-03-04", 4L),
      (10L, "2017-04-14", 4L)
    ).toDF(cols: _*)
  }

  def testHotelsForecastData: Dataset[HotelForecast] = {
    Seq(
      HotelForecast(4, Hotel(1, "First", "Rashka", "Moskow", "Pupkina st. 228", 14.88, 3.22, "pisos"), null),
      HotelForecast(5, Hotel(2, "Second", "Belarashka", "Memsk", "Kuprevicha st. 1/1", 22.8, 66.6, "pezdec"), null),
      HotelForecast(3, Hotel(3, "Third", "Ukraine", "Kiev", "Zdorovenki buli st. 666", 42.42, 10.10, "CTOXyEB"), null),
      HotelForecast(3, Hotel(4, "Fourth", "Ukraine", "Zhitomir", "Puker st. 15", 10.10, 42.42, "geocache"), null)
    ).toDS()
  }

  test("idleDaysCalculating") {
    //given
    val data = testExpediaData
    val expectedCols = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("hotel_id", LongType, nullable = false),
        StructField("srch_ci", StringType, nullable = true),
        StructField("prev_srch_ci", StringType, nullable = true),
        StructField("idle_days", IntegerType, nullable = true)
      )
    )
    val expectedResult = spark.createDataFrame(
      Seq[(Long, Long, String, String, Integer)](
        (1L, 1L, "2016-05-27", "2016-04-28", 29),
        (2L, 1L, "2016-04-28", "2016-03-28", 31),
        (3L, 1L, "2016-03-28", null, null),
        (4L, 2L, "2017-01-14", null, null),
        (5L, 2L, "2017-02-27", "2017-01-14", 44),
        (6L, 2L, "2017-11-13", "2017-02-27", 259),
        (7L, 3L, "2018-01-01", null, null),
        (8L, 3L, "2018-01-01", "2018-01-01", 0),
        (9L, 4L, "2017-03-04", null, null),
        (10L, 4L, "2017-04-14", "2017-03-04", 41)
      ).toDF("id", "hotel_id", "srch_ci", "prev_srch_ci", "idle_days").rdd, expectedCols)

    //when
    val result = calculateIdleDaysForEveryHotel(data).orderBy($"id")

    //then
    assert(result.schema.contains(StructField("prev_srch_ci", StringType)))
    assert(result.schema.contains(StructField("idle_days", IntegerType)))
    assertDataFrameEquals(expectedResult, result)
  }

  test("validExpediaData") {
    //given
    val data = calculateIdleDaysForEveryHotel(testExpediaData)
    val expectedCols = StructType(
      Seq(
        StructField("hotel_id", LongType, nullable = false),
        StructField("id", LongType, nullable = false),
        StructField("srch_ci", StringType, nullable = true),
        StructField("prev_srch_ci", StringType, nullable = true),
        StructField("idle_days", IntegerType, nullable = true)
      )
    )
    val expectedResult = spark.createDataFrame(Seq[(Long, Long, String, String, Integer)](
      (2L, 4L, "2017-01-14", null, null),
      (2L, 5L, "2017-02-27", "2017-01-14", 44),
      (2L, 6L, "2017-11-13", "2017-02-27", 259),
      (3L, 7L, "2018-01-01", null, null),
      (3L, 8L, "2018-01-01", "2018-01-01", 0),
      (4L, 9L, "2017-03-04", null, null),
      (4L, 10L, "2017-04-14", "2017-03-04", 41)
    ).toDF("hotel_id", "id", "srch_ci", "prev_srch_ci", "idle_days").rdd, expectedCols)

    //when
    val result = getExpediaForHotelsWithValidDataOnly(data).orderBy($"id")

    //then
    assertDatasetEquals(result, expectedResult)
  }

  test("invalidHotels") {
    //given
    val expediaData = calculateIdleDaysForEveryHotel(testExpediaData)
    val hotelForecast = testHotelsForecastData
    val expectedResult = Seq(
      Hotel(1, "First", "Rashka", "Moskow", "Pupkina st. 228", 14.88, 3.22, "pisos")
    ).toDF()

    //when
    val result = getInvalidHotelsData(expediaData, hotelForecast)

    //then
    assertDatasetEquals(result, expectedResult)
  }

  test("countBookingsGroupedByCountryAndCity") {
    //given
    val validExpedia = getExpediaForHotelsWithValidDataOnly(calculateIdleDaysForEveryHotel(testExpediaData))
    val expectedResult = Seq(
      ("Belarashka", "Memsk", 3L),
      ("Ukraine", "Kiev", 2L),
      ("Ukraine", "Zhitomir", 2L)
    ).toDF("country", "city", "count")

    //when
    val result = getTheRemainingDataAndBookingCounts(validExpedia, testHotelsForecastData)

    //then
    assertDataFrameEquals(expectedResult, result)
  }
}
