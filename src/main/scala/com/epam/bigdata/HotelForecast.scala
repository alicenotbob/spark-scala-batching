package com.epam.bigdata

case class HotelForecast(precision: Short, hotel: Hotel, weather: Weather)

case class Hotel(id: Long, name: String, country: String, city: String, address: String, latitude: Double, longitude: Double, geohash: String)

case class Weather (lng: Double, lat: Double, avg_tmpr_f: Double, avg_tmpr_c: Double, wthr_date: String, year: Integer, month: Short, day: Short)