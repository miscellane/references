package com.grey.wiki

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.sql.types.{StringType, StructType}

class SettingStrings() {
  
  def settingStrings(data: DataFrame, schema: StructType): DataFrame = {

    val stringFields: Seq[String] = schema.map{ field =>
      if (field.dataType == StringType){
        field.name
      } else {
        ""
      }
    }

    var dataCopy: DataFrame = data
    stringFields.filter(!_.isEmpty).foreach{ field =>
      val temporaryName: String = "lower" + s"_$field"
      dataCopy = dataCopy.withColumn( temporaryName, lower(col(field)) )
        .drop(field).withColumnRenamed(temporaryName, field)
    }
    dataCopy.show()
    
    dataCopy
    
  }

}
