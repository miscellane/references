package com.grey.wiki

import java.io.File

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try
import scala.util.control.Exception


class DataSteps(spark: SparkSession) {

  private val localSettings = new LocalSettings()
  private val dataSchema = new DataSchema()

  def dataSteps(): Unit = {


    import spark.implicits._


    // Data Path
    val dataDirectory: String = localSettings.dataDirectory
    val fileObject: File = new File(dataDirectory + "2015_2_clickstream.tsv")
    val sizeGB: Double = fileObject.length().toDouble / 1000000000.0
    println(f"$sizeGB%.6f GB")

    // println("Scala: " + Source.fromFile(dataDirectory + "2015_2_clickstream.tsv").length)


    // The schema of the data set
    val schema: StructType = dataSchema.schema


    // Read-in the data
    val clicks: DataFrame = spark.read.schema(schema).format("csv")
      .option("sep", "\t")
      .option("header", "true")
      .load(dataDirectory + "*.tsv")


    // Schema
    clicks.printSchema()


    // Inspect
    val dataInspection = new DataInspection()
    dataInspection.dataInspection(data = clicks, schema = schema)


    // Strings: convert all strings to lower case
    val settingStrings = new SettingStrings()
    val clicksObject: Try[DataFrame] = Exception.allCatch.withTry(
      settingStrings.settingStrings(data = clicks, schema = schema)
    )


    // Hence
    val dataExplore = new DataExplore(spark)
    val exploring = if (clicksObject.isSuccess) {
      Exception.allCatch.withTry(
        dataExplore.dataExplore(clicksObject.get)
      )
    } else {
      sys.error(clicksObject.failed.get.getMessage)
    }


    // Extra feature: crazy
    val featureEngineering = new FeatureEngineering(spark)
    if (exploring.isSuccess) {
      featureEngineering.featureEngineering(clicksObject.get)
    }


    // Data Modelling Type
    val (settingTypes: DataFrame, dim_link_type: DataFrame) = if (exploring.isSuccess) {
      new DataModellingType(spark).dataModellingType(clicksObject.get)
    } else {
      sys.error(exploring.failed.get.getMessage)
    }


    // Data Modelling Source
    val (settingSources: DataFrame, dim_source: DataFrame) = new DataModellingSource(spark)
      .dataModellingSource(settingTypes)


    // Data Modelling Requirements
    // f_links Fact Table: n, (curr_title).as(title), fk_type, fk_source
    // dim_link_type Dimension Table: fk_type, type
    // dim_source Dimension Table: fk_source, prev_title
    val f_links_fields = List("fk_type", "fk_source", "n", "curr_title")
    val f_links = settingSources.selectExpr(f_links_fields: _*)
      .withColumnRenamed("curr_title", "title")

    dim_link_type.show()
    dim_source.show()
    f_links.show()


    // Inspections
    println("\n\nInspections ...")
    f_links.filter($"fk_type".isNull).show()
    f_links.select($"fk_type").distinct().show()

    f_links.filter($"fk_source".isNull).show()
    f_links.select($"fk_source").distinct().show()


  }


}
