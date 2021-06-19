package com.grey.wiki

class LocalSettings {

  // The operating system
  val operatingSystem: String = System.getProperty("os.name").toUpperCase
  val operatingSystemWindows: Boolean =  operatingSystem.startsWith("WINDOWS")


  // Local project directory, and directory separation convention
  val projectDirectory: String = System.getProperty("user.dir")
  val localSeparator: String = System.getProperty("file.separator")


  // Local data directories
  val warehouseDirectory: String = s"$projectDirectory${localSeparator}spark-warehouse$localSeparator"
  val dataDirectory: String = s"$projectDirectory${localSeparator}data$localSeparator"

}
