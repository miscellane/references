lazy val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.11.12"
)

lazy val root = (project in file(".")).
  settings(commonSettings:_*).
  settings(
    name := "derma",

    resolvers ++= Seq(
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "Redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release",
      "Typesafe" at "http://repo.typesafe.com/typesafe/releases/",
      "Java.net Maven2 Repository" at "http://download.java.net/maven/2/"
    ),

    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.11" % "2.4.4",
      "org.apache.spark" % "spark-sql_2.11" % "2.4.4",
      "org.apache.hadoop" % "hadoop-aws" % "2.7.5",
      "org.apache.logging.log4j" % "log4j-core" % "2.11.2",
      "commons-codec" % "commons-codec" % "1.12",
      "org.apache.commons" % "commons-io" % "1.3.2",
      "joda-time" % "joda-time" % "2.10.1",
      "org.joda" % "joda-convert" % "2.1.2"
    ),

    unmanagedBase := baseDirectory.value / "../../libraries/",

    mainClass in assembly := Some("com.grey.fundamentals.BaselineApp")




  )