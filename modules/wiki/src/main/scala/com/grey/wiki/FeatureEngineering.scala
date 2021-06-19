package com.grey.wiki

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

import scala.util.matching.Regex

class FeatureEngineering(spark: SparkSession) {


  def featureEngineering(clicks: DataFrame): Unit = {

    // This import is required for
    //    the $-notation
    //    implicit conversions like converting RDDs to DataFrames
    //    encoders for most common types, which are automatically provided by importing spark.implicits._
    import spark.implicits._


    // Regular Expression: One or more digits
    val exp: Regex = "[0-9]+".r


    // The crazy user defined function
    val crazy: UserDefinedFunction = udf((text: String) => {

      val seqOfNumbers: Option[String] = exp.findFirstIn(text)
      val hasDigits = seqOfNumbers match {
        case Some(y) => true
        case None => false
      }

      val singleX = text.count( _ == 'x') == 1
      val singleY = text.count( _ == 'y') == 1

      if (hasDigits && singleX && singleY) 1 else 0

    })


    /*
    println("A preview of crazy cases: ")
    clicks.withColumn("crazy", crazy($"curr_title"))
      .filter($"crazy" === 1).show()
    */


    println("\n\nA preview of the data after appending the 'crazy' column: ")
    clicks.withColumn("crazy", crazy($"curr_title"))
      .show()


  }


}
