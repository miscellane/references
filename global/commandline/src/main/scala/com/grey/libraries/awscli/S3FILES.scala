package com.grey.libraries.awscli

class S3FILES(operatingSystemWindows: Boolean) {

  def getListOfFiles(bucketString: String): List[String] = {

    // Build command
    val commandString: String = s"""aws s3 ls $bucketString --recursive"""

    // Run command
    val output: String = if (operatingSystemWindows) {
      scala.sys.process.stringToProcess("cmd /C " + commandString).!!
    } else {
      scala.sys.process.stringToProcess(commandString).!!
    }

    // Convert the output to a list
    val listOfFiles: List[String] = output.split("\n").toList.par.map(x => x.split(" ").reverse.head).toList

    // Return
    listOfFiles

  }

}
