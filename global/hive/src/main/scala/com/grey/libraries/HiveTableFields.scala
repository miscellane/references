package com.grey.libraries

import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.Try
import scala.util.control.Exception

class HiveTableFields {


  def hiveTableFields(fieldsStructType: StructType): String = {


    // Pending '.toDDL' switch; code section further below.  From Spark 2.4.+ onward the function StructField.toDDL
    // converts a field's properties to the required string format, i.e., nameOfField hiveDataType COMMENT 'comment'


    // The instance for converting Spark DataTypes to their Hive counterparts.
    val hiveDataType = new HiveDataType()


    // Building a Hive table's strings of fields
    val F: Try[Seq[String]] = Exception.allCatch.withTry(
      fieldsStructType.map { fieldProperties: StructField =>
        fieldProperties.name + " " +
          hiveDataType.hiveDataType(fieldProperties.dataType) +
          " COMMENT '" + fieldProperties.getComment.orNull + "'"
      }
    )
    if (F.isSuccess) {
      F.get.mkString(",")
    } else {
      sys.error("Error: " + F.failed.get.getMessage)
    }


    // Cf. stringOfFields.get & F.get.mkString.  If equal, switch to 'toDDL'
    /*
    val stringOfFields: Try[String] = Exception.allCatch.withTry(
      fieldsStructType.toDDL
    )
    if (stringOfFields.isSuccess){
      stringOfFields.get
    } else {
      sys.error("Error: " + stringOfFields.failed.get.getMessage)
    }
    */


  }


}
