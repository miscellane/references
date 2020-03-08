package com.grey.fundamentals

import java.io.File
import java.net.URL

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.util.Try
import scala.util.control.Exception

/**
  *
  * @param spark: The SparkSession
  */
class DataMetadata(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val resourcesDirectory = localSettings.resourcesDirectory
  private val dataDirectory = localSettings.dataDirectory


  /**
    * Prospective package input parameters declared within a YAML
    *   fileName
    *   schemaFile of fileName
    *   rootURL, i.e., https://raw.githubusercontent.com/greyhypotheses/dermatology/master/data/
    * @return
    */
  def dataMetadata(): DataFrame = {


    val fileName = "isic_2019_metadata.csv"
    val metadataURL: String = s"https://raw.githubusercontent.com/greyhypotheses/dermatology/master/data/$fileName"


    // Download the metadata
    val getMetadata: Try[Unit] = Exception.allCatch.withTry(
      FileUtils.copyURLToFile(new URL(metadataURL), new File(dataDirectory + fileName))
    )
    if (getMetadata.isFailure) {
      sys.error(getMetadata.failed.get.getMessage)
    }


    // Schema
    val fieldsProperties: Try[RDD[String]] = Exception.allCatch.withTry(
      spark.sparkContext
        .textFile(resourcesDirectory + "metadata.json")
    )


    // Converting the fields details to StructType form
    val fieldsStructType: StructType = if (fieldsProperties.isSuccess) {DataType
      .fromJson(fieldsProperties.get.collect.mkString("")).asInstanceOf[StructType]
    } else {
      sys.error("Error: " + fieldsProperties.failed.get.getMessage)
    }


    // Reading-in the data
    val metadata: DataFrame =  spark.read.schema(fieldsStructType)
      .option("header", "true")
      .csv(dataDirectory + fileName)


    // Return
    metadata


  }

}
