package com.grey.events

import org.apache.spark.sql.SparkSession

class DataSteps(spark: SparkSession) {

  def dataSteps(): Unit = {

    val ablyWeather = new AblyWeather(spark)
    ablyWeather.ablyWeather()

    // val ablyWeatherEvents = new AblyWeatherEvents(spark)
    // ablyWeatherEvents.ablyWeatherEvents()

    // val ablyBitCoin = new AblyBitCoin()
    // ablyBitCoin.ablyBitCoin()

    // val ablyTube = new AblyTube()
    // ablyTube.ablyTube()

  }

}
