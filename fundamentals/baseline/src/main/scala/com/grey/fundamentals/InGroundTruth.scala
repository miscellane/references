package com.grey.fundamentals

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.storage.StorageLevel

class InGroundTruth(spark: SparkSession) {

  def inGroundTruth(data: DataFrame, labels: Array[String]): Unit = {


    import spark.implicits._


    // Prepare
    var truth: DataFrame = data
    truth.persist(StorageLevel.MEMORY_ONLY)


    // How many classes has each record been assigned to?
    val integerFields: Seq[Column] = labels.map(x => col(x))
    truth = truth.withColumn("nClasses", integerFields.reduce(_ + _))
    val classes: Long = truth.select(sum($"nClasses").as("N")).first().getAs[Long]("N")

    println("\nNumber of assigned classes: " + classes)
    println("Number of records: " + truth.count())


    // Number of records per label
    val aggregator: Seq[Column] = integerFields.map(x => sum(x))
    val instancesPerLabel: DataFrame = truth.agg(aggregator.head, aggregator.tail: _*)

    println("\nInstances per label")
    instancesPerLabel.show()
    println("Sum of instances per label: " + instancesPerLabel.first().toSeq.asInstanceOf[Seq[Long]].sum)


  }

}
