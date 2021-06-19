package com.grey.events

import java.io.{File, FileInputStream}

import net.liftweb.json.{DefaultFormats, JValue, JsonParser}

import scala.util.Try
import scala.util.control.Exception

class KeyReader {

  private val localSettings = new LocalSettings()

  def keyReader(apiName: String): String = {


    // Get the key values from a parameter file named
    // keyValues.json, which must exist within the same
    // directory that this program's JAR is placed.
    val keysInputStream: Try[FileInputStream] = Exception.allCatch.withTry(
      new FileInputStream(new File(localSettings.projectDirectory +
        localSettings.localSeparator + localSettings.path + "keyValues.json"))
    )


    // Extract the JSON string
    val jsonString: String = if (keysInputStream.isSuccess) {
      scala.io.Source.fromInputStream(keysInputStream.get).mkString
    } else {
      sys.error("Error: " + keysInputStream.failed.get.getMessage)
    }


    // The values
    implicit val formats: DefaultFormats.type = DefaultFormats
    val liftJValue: JValue = JsonParser.parse(jsonString)
    val T: KeyValues = liftJValue.extract[KeyValues]


    // Match
    val value: Try[String] = Exception.allCatch.withTry(
      apiName match {
        case "ably" => T.ably
        case "dpla" => T.dpla
        case "quandl" => T.quandl
        case _ => sys.error("Unknown API")
      }
    )


    // Does the key of interest exist?
    if (value.isSuccess) {
      value.get
    } else {
      sys.error(value.failed.get.getMessage)
    }

  }

  case class KeyValues(ably: String, dpla: String, quandl: String)

}
