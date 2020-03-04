package com.grey.wiki

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.types.{IntegerType, StructType}

import scala.collection.parallel.ParSeq
import scala.collection.parallel.mutable.ParArray

class DataInspection() {

  def dataInspection(data: DataFrame, schema: StructType): Unit = {

    // Nulls per field
    val numberOfNullCases: ParSeq[(String, Long)] = data.columns.par.map { field =>
      val numberOf: Long = data.filter(col(field).isNull).count()
      (field, numberOf)
    }
    println("Number of null instances per column:")
    println(numberOfNullCases.toList)


    // Empty
    val numberOfEmptyCases: ParArray[(String, Long)] = data.columns.par.map{ field =>
      val numberOf: Long = data.filter(length(col(field)) === 0).count()
      (field, numberOf)
    }
    println("\nNumber of empty instances per column:")
    println(numberOfEmptyCases.toList)


    // Integers & Zero
    val integersAndZeros: ParSeq[(String, Long)] = schema.par.map{ field =>
      if (field.dataType == IntegerType) {
        val numberOf: Long = data.filter(col(field.name) === 0).count()
        (field.name, numberOf)
      } else {
        (field.name, -1.toLong)
      }
    }
    println("\nNumber of zero instances per integer type column: ")
    println(integersAndZeros.toList.filter(x => x._2 >= 0))


    val addendum: ParSeq[(String, Long)] = schema.par.map{ field =>
      if (field.dataType == IntegerType) {
        val numberOf: Long = data.filter(col(field.name).isNaN).count()
        (field.name, numberOf)
      } else {
        (field.name, -1.toLong)
      }
    }
    println("\nNumber of NaN instances per integer type column: ")
    println(addendum.toList.filter(x => x._2 >= 0))

    val post: ParSeq[(String, Long)] = schema.par.map{ field =>
      if (field.dataType == IntegerType) {
        val numberOf: Long = data.filter(col(field.name) > 0).count()
        (field.name, numberOf)
      } else {
        (field.name, -1.toLong)
      }
    }
    println("\nNumber of normal instances per integer type column: ")
    println(post.toList.filter(x => x._2 >= 0))


  }


}
