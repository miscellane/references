package com.grey.libraries.awscli

import scala.util.Try
import scala.util.control.Exception

class S3(operatingSystemWindows: Boolean) {


  // Remove, i.e., delete, files within a specified S3 directory
  // filesConstraints => --exclude "" --include "" --recursive
  def RM(bucketString: String, filesConstraints: String = ""): Try[Int] = {

    val commandString = s"""aws s3 rm $bucketString $filesConstraints"""

    val action: Try[Int] = Exception.allCatch.withTry(
      if (operatingSystemWindows) {
        scala.sys.process.stringToProcess("cmd /C " + commandString).!
      } else {
        scala.sys.process.stringToProcess(commandString).!
      }
    )

    if (action.isFailure) {
      sys.error("Error: " + action.failed.get.getMessage)
    } else {
      action
    }

  }


  // List the files within a specified S3 directory
  // filesConstraints => --recursive
  def LS(bucketString: String, filesConstraints: String = ""): Try[Int] = {

    val commandString = s"""aws s3 ls $bucketString $filesConstraints"""

    val action: Try[Int] = Exception.allCatch.withTry(
      if (operatingSystemWindows) {
        scala.sys.process.stringToProcess("cmd /C " + commandString).!
      } else {
        scala.sys.process.stringToProcess(commandString).!
      }
    )

    if (action.isFailure) {
      sys.error("Error: " + action.failed.get.getMessage)
    } else {
      action
    }

  }


  // CP
  // filesConstraints => --exclude "" --include "" --recursive
  def CP(sourceString: String, targetString: String,
         filesConstraints: String = ""): Try[Int] = {

    val commandString = s"""aws s3 cp $sourceString $targetString $filesConstraints"""

    val action: Try[Int] = Exception.allCatch.withTry(
      if (operatingSystemWindows) {
        scala.sys.process.stringToProcess("cmd /C " + commandString).!
      } else {
        scala.sys.process.stringToProcess(commandString).!
      }
    )

    if (action.isFailure) {
      sys.error("Error: " + action.failed.get.getMessage)
    } else {
      action
    }

  }


  // Move files to S3
  // filesConstraints => --exclude "" --include "" --recursive
  def MV(sourceString: String, targetString: String,
         filesConstraints: String = ""): Try[Int] = {

    val commandString = s"""aws s3 mv $targetString $sourceString $filesConstraints"""

    val action: Try[Int] = Exception.allCatch.withTry(
      if (operatingSystemWindows) {
        scala.sys.process.stringToProcess("cmd /C " + commandString).!
      } else {
        scala.sys.process.stringToProcess(commandString).!
      }
    )

    if (action.isFailure) {
      sys.error("Error: " + action.failed.get.getMessage)
    } else {
      action
    }


  }


}
