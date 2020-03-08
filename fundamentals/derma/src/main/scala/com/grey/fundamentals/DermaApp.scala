package com.grey.fundamentals

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.util.Try

object DermaApp {

  val localSettings = new LocalSettings()

  def main(args: Array[String]): Unit = {

    // Minimise Spark & Logger Information
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Spark Session Instance
    // .config("spark.master", "local")
    // .config("spark.kryo.registrationRequired", "true")
    val spark = SparkSession.builder().appName("Demo")
      .config("spark.sql.warehouse.dir", localSettings.warehouseDirectory)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.master", "local")
      .getOrCreate()

    // Spark Context Level Logging
    spark.sparkContext.setLogLevel("ERROR")


    // Configurations Parameters
    // A ... Master Node will be used. Probably 4 cores available
    val nShufflePartitions = 8
    val nParallelism = 8


    // Configurations
    // sql.shuffle.partitions: The number of shuffle partitions for joins & aggregation
    // default.parallelism: The default number of partitions delivered after a transformation
    // spark.conf.set("spark.speculation", value = false)
    spark.conf.set("spark.sql.shuffle.partitions", nShufflePartitions.toString)
    spark.conf.set("spark.default.parallelism", nParallelism.toString)
    spark.conf.set("spark.kryoserializer.buffer.max", "2048m")


    // Proceed
    val dataSteps = new DataSteps(spark)
    dataSteps.dataSteps()



  }

}
