package com.grey.wiki

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{monotonically_increasing_id, udf}
import org.apache.spark.storage.StorageLevel

class DataModellingSource(spark: SparkSession) {
  

  def dataModellingSource(data: DataFrame): (DataFrame, DataFrame) = {

    // This import is required for
    //    the $-notation
    //    implicit conversions like converting RDDs to DataFrames
    //    encoders for most common types, which are automatically provided by importing spark.implicits._
    import spark.implicits._

    
    // The data
    val links = data
    links.persist(StorageLevel.MEMORY_ONLY)


    // Source: prev_title neither has null not empty instances
    val labelledReferrers = List("other-empty", "other-internal", "other-google", "other-yahoo",
      "other-bing", "other-facebook", "other-twitter", "other-other") ::: List("other-wikipedia")


    // Setting-up prev_title    
    val source: UserDefinedFunction = udf((referrer: String) => {
      referrer match {
        case x if labelledReferrers.count(_ == x) == 1 => x
        case _ => "internal-wikipedia"
      }
    })


    // Note: The expected number of internal-wikipedia records === links.filter(!isin(labelledReferrers: _*).count()
    val resettingLinks = links.withColumn("source", source($"prev_title"))
      .drop($"prev_title").withColumnRenamed("source", "prev_title")

    
    // dim_source
    val dim_source = resettingLinks.select($"prev_title").distinct()
      .sort($"prev_title").coalesce(1)
      .withColumn("fk_source", monotonically_increasing_id())

    
    // Appending fk_source
    val extendedData = resettingLinks.join(dim_source, Seq("prev_title"), "left_outer")

    
    // Return
    (extendedData, dim_source)
    
  }

}
