package com.grey.libraries

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try
import scala.util.control.Exception


class HiveLayer(spark: SparkSession)  {


  def hiveLayer(hiveProperties: HiveLayerCaseClass#HLCC, stringOfFields: String): List[Try[DataFrame]] = {


    // Queries
    val createDatabase =
      s"""
         |CREATE DATABASE IF NOT EXISTS ${hiveProperties.databaseName}
         |COMMENT '${hiveProperties.commentDatabase}'
         |LOCATION '${hiveProperties.databaseLocation}'
         |WITH DBPROPERTIES (${hiveProperties.databaseProperties})
       """.stripMargin


    val createTable =
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveProperties.databaseName}.${hiveProperties.tableName}
         |(
         |  $stringOfFields
         |)
         |COMMENT '${hiveProperties.commentTable}'
         |PARTITIONED BY ( ${hiveProperties.partitionString} )
         |${hiveProperties.bucketClause}
         |STORED AS ${hiveProperties.storedAs}
         |LOCATION '${hiveProperties.tableLocation}'
         |TBLPROPERTIES ('serialization.null.format' = "")
        """.stripMargin


    val dropPartitionQuery =
      s"""
         |ALTER TABLE ${hiveProperties.databaseName}.${hiveProperties.tableName}
         |DROP IF EXISTS PARTITION (${hiveProperties.partitionSpecification})
       """.stripMargin


    val addPartitionQuery =
      s"""
         |ALTER TABLE ${hiveProperties.databaseName}.${hiveProperties.tableName}
         |ADD PARTITION (${hiveProperties.partitionSpecification})
         |LOCATION '${hiveProperties.partitionOfInterest}'
       """.stripMargin


    List(createDatabase, createTable, dropPartitionQuery, addPartitionQuery).map{ queryString =>
      val T: Try[DataFrame] = Exception.allCatch.withTry(
        spark.sql(queryString)
      )
      if (T.isFailure) {
        sys.error(T.failed.get.getMessage)
      } else {
        T
      }
    }


  }


}
