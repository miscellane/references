package com.grey.wiki

import org.apache.spark.sql.functions.{monotonically_increasing_id, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class DataModellingType(spark: SparkSession) {
  /*
    Herein Scala naming conventions are broken whenever there is a clash with the exercise requests
  */

  def dataModellingType(data: DataFrame): (DataFrame, DataFrame) = {

    // This import is required for
    //    the $-notation
    //    implicit conversions like converting RDDs to DataFrames
    //    encoders for most common types, which are automatically provided by importing spark.implicits._
    import spark.implicits._


    // The data
    val links = data
    links.persist(StorageLevel.MEMORY_ONLY)


    // Type: type has a few null instances
    val resettingLinks = links.withColumn("type_setting", when($"type".isNull, "unknown").otherwise($"type"))
      .drop($"type").withColumnRenamed("type_setting", "type")


    // Herein coalesce limits the primary key range; useful in the case of tiny tables
    val dim_link_type = resettingLinks.select($"type").distinct().sort($"type").coalesce(1)
      .withColumn("fk_type", monotonically_increasing_id())


    // Appending fk_type
    val extendedData = resettingLinks.join(dim_link_type, Seq("type"), "left_outer")


    // Return
    (extendedData, dim_link_type)


  }

}
