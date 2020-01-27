package com.grey.hive

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try
import scala.util.control.Exception

class ClearUp(spark: SparkSession) {

  def clearUp(hiveProperties: HiveLayerCaseClass#HLCC): Unit = {


    // Foremost, ensure the table's database exists, if not, create it.
    val createDatabase =
      s"""
         |CREATE DATABASE IF NOT EXISTS ${hiveProperties.databaseName}
         |COMMENT '${hiveProperties.commentDatabase}'
         |LOCATION '${hiveProperties.databaseLocation}'
         |WITH DBPROPERTIES (${hiveProperties.databaseProperties})
       """.stripMargin

    val isCreateDatabase: Try[DataFrame] = Exception.allCatch.withTry(
      spark.sql(createDatabase)
    )


    // Next, get the array of tables in the database.  Of course, this
    // will be empty at the beginning of time ...

    val arrayOfTables: Array[String] = spark.sqlContext.tableNames(hiveProperties.databaseName)
    val tableOfInterest: Array[String] = arrayOfTables.filter(_ == hiveProperties.tableName)


    // Conditional partition drop.  If the table in question exists, drop the
    // partition in question.
    val dropPartitionQuery =
    s"""
       |ALTER TABLE ${hiveProperties.databaseName}.${hiveProperties.tableName}
       |  DROP IF EXISTS PARTITION (${hiveProperties.partitionSpecification})
       """.stripMargin

    if (tableOfInterest.length > 0) {
      spark.sql(dropPartitionQuery)
    }


  }

}
