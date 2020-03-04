package com.grey.wiki

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataExplore(spark: SparkSession) {

  private val formatter = java.text.NumberFormat.getIntegerInstance

  def dataExplore(clicks: DataFrame): Unit = {

    import spark.implicits._


    // Descriptive statistics
    val numberOfRequests = clicks.select(sum($"n").as("n")).head().getAs[Long]("n")

    println("Number of records, i.e., unique referrer/resource pairings: " + formatter.format(clicks.count()) )
    println("\nNumber of requests, i.e., search/redirect occurrences: " + formatter.format(numberOfRequests) )


    // Bing, Yahoo
    val directions: String => Long = (x: String) => {
      clicks.filter($"prev_title" === x)
        .select(sum($"n").as("n")).head().getAs[Long]("n")
    }

    val nBing: Long = directions("other-bing")
    val nYahoo = directions("other-yahoo")
    println("\nDirections courtesy of Bing & Yahoo")
    println("Bing: " + formatter.format(nBing))
    println("Yahoo: " + formatter.format(nYahoo))


    // Trends
    val sourceTwitter = clicks.filter($"prev_title" === "other-twitter")
    val summaryTwitter = sourceTwitter.groupBy($"curr_title").agg(sum($"n").as("n")).sort($"n".desc)
    println("\nTwitter's top trending topics:")
    summaryTwitter.show()


    // From English Wikipedia
    // prev_title does not have any null or empty instances
    val excl: List[String] = List("other-empty", "other-internal", "other-google", "other-yahoo",
      "other-bing", "other-facebook", "other-twitter", "other-other")
    val wikipedia = clicks.filter(!$"prev_title".isin(excl: _*))

    println("Percentage from English Wikipedia")
    val nWikipedia = wikipedia.select(sum($"n").as("n")).head().getAs[Long]("n")
    val percentage = 100 * nWikipedia.toDouble / numberOfRequests.toDouble
    println(f"English Wikipedia Percentage: $percentage%.2f" )


    // Inspecting
    /*
    println("Effective 'from english wikipedia' filter?")
    wikipedia.filter($"prev_title".isin(excl: _*)).show()

    println("From English Wikipedia")
    wikipedia.show()
    */


  }


}
