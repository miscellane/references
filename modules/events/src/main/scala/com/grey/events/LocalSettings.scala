package com.grey.events

class LocalSettings {

  // System
  val projectDirectory: String = System.getProperty("user.dir")
  val localSeparator: String = System.getProperty("file.separator")

  // Application Path
  val path: String = s"src${localSeparator}main${localSeparator}scala" +
    s"${localSeparator}com${localSeparator}grey${localSeparator}events$localSeparator"

  // Data
  val resourcesDirectory: String = s"$projectDirectory${localSeparator}src" +
    s"${localSeparator}main${localSeparator}resources$localSeparator"
  val dataDirectory: String = projectDirectory + localSeparator + "data" + localSeparator

}
