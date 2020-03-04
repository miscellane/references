package com.grey.fundamentals

import java.io.File
import java.net.URL

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructType}
import org.apache.spark.sql.functions.col
import org.apache.spark.storage.StorageLevel

import scala.util.Try
import scala.util.control.Exception

/**
  *
  * @param spark: The SparkSession
  */
class DataGroundTruth(spark: SparkSession) {


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
  def dataGroundTruth(): DataFrame = {


    // Download the file of interest and save to a local location
    val fileName = "isic_2019_ground_truth.csv"
    val groundTruthURL: String = s"https://raw.githubusercontent.com/greyhypotheses/dermatology/master/data/$fileName"

    val getGroundTruth: Try[Unit] = Exception.allCatch.withTry(
      FileUtils.copyURLToFile(new URL(groundTruthURL), new File(dataDirectory + fileName) )
    )
    if (getGroundTruth.isFailure) {
      sys.error(getGroundTruth.failed.get.getMessage)
    }


    // Schema
    val fieldsProperties: Try[RDD[String]] = Exception.allCatch.withTry(
      spark.sparkContext
        .textFile(resourcesDirectory + "groundTruth.json")
    )


    // Converting the fields details to StructType form
    val fieldsStructType: StructType = if (fieldsProperties.isSuccess) {DataType
      .fromJson(fieldsProperties.get.collect.mkString("")).asInstanceOf[StructType]
    } else {
      sys.error("Error: " + fieldsProperties.failed.get.getMessage)
    }


    // Import
    val dataset: DataFrame =  spark.read.schema(fieldsStructType)
      .option("header", "true")
      .csv(dataDirectory + fileName)


    // The labels should be integers.  Hence, via a new data frame
    var truth: DataFrame = dataset
    truth.persist(StorageLevel.MEMORY_ONLY)


    // ... and after identifying the label fields
    val doubleFields: Seq[String] = fieldsStructType.map{ field =>
      if (field.dataType == DoubleType){
        field.name
      } else {
        ""
      }
    }


    // ... re-cast each label field to an IntegerType
    doubleFields.filter(!_.isEmpty).foreach{ field =>
      val temporaryName: String = "cast" + s"_$field"
      truth = truth.withColumn( temporaryName, col(field).cast(IntegerType) )
        .drop(field)
        .withColumnRenamed(temporaryName, field)
    }


    // Return
    truth


  }

}
