package com.grey.hive

class HiveLayerCaseClass {

  case class HLCC(
                   databaseName: String,
                   commentDatabase: String,
                   databaseProperties: String,
                   databaseLocation: String,
                   tableName: String,
                   commentTable: String,
                   tableLocation: String,
                   storedAs: String,
                   partitionOfInterest: String,
                   partitionString: String,
                   partitionSpecification: String,
                   bucketClause: String
                 )

}
