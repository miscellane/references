package com.grey.fundamentals

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.storage.StorageLevel

class DataSteps(spark: SparkSession) {

  def dataSteps(): Unit = {

    import spark.implicits._

    val dataGroundTruth = new DataGroundTruth(spark)
    val groundTruth: DataFrame = dataGroundTruth.dataGroundTruth()
    groundTruth.show(5)

    val dataMetadata = new DataMetadata(spark)
    val metadata: DataFrame = dataMetadata.dataMetadata()
    metadata.show(5)

    val labels: Array[String] = groundTruth.columns.filter(!_.matches("image"))
    val inGroundTruth = new InGroundTruth(spark)
    inGroundTruth.inGroundTruth(data = groundTruth, labels = labels)

    val dataset = metadata.join(groundTruth, Seq("image"), "left_outer")
    dataset.persist(StorageLevel.MEMORY_ONLY)
    println()
    dataset.show(5)


    val aggregators: Array[Column] = labels.map(x => sum(col(x)))
    dataset.groupBy($"anatom_site_general").agg(aggregators.head, aggregators.tail: _*).show()
    dataset.groupBy($"sex").agg(aggregators.head, aggregators.tail: _*).show()
    dataset.groupBy($"age_approx").agg(aggregators.head, aggregators.tail: _*).show()

  }

}
