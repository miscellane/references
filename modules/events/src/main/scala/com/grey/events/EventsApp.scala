package com.grey.events

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.Try
import scala.util.control.Exception

// import org.apache.spark.SparkConf
// import org.apache.spark.streaming.{Seconds, StreamingContext}


object EventsApp {

  private val localSettings = new LocalSettings()

  def main(args: Array[String]): Unit = {

    // Minimise logging outputs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Spark Session Instance
    // .config("spark.master", "local[*]")
    val spark = SparkSession.builder()
      .appName("Events")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // Log Level: WARN, ERROR
    spark.sparkContext.setLogLevel("ERROR")

    // The streamed data host
    val dataDirectoryObject = new File(localSettings.dataDirectory)

    // Clear-up
    val clearing: Try[Unit] = Exception.allCatch.withTry(
      if (dataDirectoryObject.exists()) {FileUtils.deleteDirectory(dataDirectoryObject)}
    )
    println(clearing.get)

    // Re-create directory
    val directories: Try[Boolean] = if (clearing.isSuccess){
      Exception.allCatch.withTry(
        dataDirectoryObject.mkdir()
      )
    } else {
      sys.error(clearing.failed.get.getMessage)
    }

    // Structured Streaming
    if (directories.isSuccess){
      val dataSteps = new DataSteps(spark)
      dataSteps.dataSteps()
    } else {
      sys.error(directories.failed.get.getMessage)
    }

    // Create a spark application configuration; hence set a variety of Spark parameters as key-value pairs
    // val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Events")

    // Remember, the streaming context can be created via a spark configuration, spark context, or directly via
    // a Spark Master URL & an appName
    // http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.streaming.StreamingContext
    // val streamingContext: StreamingContext = new StreamingContext(conf = sparkConf, batchDuration = Seconds(1))
    // streamingContext.sparkContext.setLogLevel("ERROR")

  }

}
