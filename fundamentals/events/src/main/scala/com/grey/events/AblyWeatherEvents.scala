package com.grey.events

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try
import scala.util.control.Exception

class AblyWeatherEvents(spark: SparkSession) {

  private val localSettings = new LocalSettings()

  def ablyWeatherEvents(): Unit = {

    // read-in the schema JSON
    val fieldProperties: Try[RDD[String]] = Exception.allCatch.withTry(
      spark.sparkContext.textFile(localSettings.resourcesDirectory + "weather.json")
    )

    // Convert the schema to a StructType
    val schema: Try[StructType] = if (fieldProperties.isSuccess){
      Exception.allCatch.withTry(
        DataType.fromJson(fieldProperties.get.collect.mkString("")).asInstanceOf[StructType]
      )
    } else {
      sys.error(fieldProperties.failed.get.getMessage)
    }

    // Read-in streams
    val data: DataFrame = spark.readStream.schema(schema.get).parquet(s"${localSettings.dataDirectory}*")

    val action: StreamingQuery = data.writeStream.format("console").start()

    action.awaitTermination()

  }

}
