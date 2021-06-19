package com.grey.events

import org.apache.spark.sql.SparkSession

/**
 *
 * @param spark: A instance of SparkSession
 */
class DataSteps(spark: SparkSession) {

  /**
   *
   */
  def dataSteps(): Unit = {

    /**
     * val ablyBitCoin = new AblyBitCoin()
     * ablyBitCoin.ablyBitCoin()
     *
     * val ablyTube = new AblyTube()
     * ablyTube.ablyTube()
     */

    val ablyWeather = new AblyWeather(spark)
    ablyWeather.ablyWeather()

  }

}
