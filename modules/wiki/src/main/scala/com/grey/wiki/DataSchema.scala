package com.grey.wiki

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

class DataSchema {

  val schema: StructType = new StructType()
    .add(name = "prev_id", dataType = IntegerType, nullable = true)
    .add(name = "curr_id", dataType = IntegerType, nullable = true)
    .add(name = "n", dataType = IntegerType, nullable = true)
    .add(name = "prev_title", dataType = StringType, nullable = true)
    .add(name = "curr_title", dataType = StringType, nullable = true)
    .add(name = "type", dataType = StringType, nullable = true)

}
