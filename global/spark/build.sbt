lazy val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.11.12"
)

lazy val root = (project in file(".")).
  settings(commonSettings:_*).
  settings(

    name := "spark",

    resolvers ++= Seq(
      "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      "Redshift" at "https://s3.amazonaws.com/redshift-maven-repository/release",
      "Typesafe" at "https://repo.typesafe.com/typesafe/releases/"
    ),

    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.11" % "2.4.4" % "provided",
      "org.apache.spark" % "spark-sql_2.11" % "2.4.4" % "provided",
      "org.apache.hadoop" % "hadoop-aws" % "2.7.5" % "provided",
      "org.apache.logging.log4j" % "log4j-core" % "2.8.2",
      "com.databricks" % "spark-redshift_2.11" % "3.0.0-preview1" % "provided",
      "com.amazonaws" % "aws-java-sdk-core" % "1.11.580" % "provided",
      "com.amazonaws" % "aws-java-sdk-s3" % "1.11.580" % "provided",
      "com.amazon.redshift" % "redshift-jdbc42" % "1.2.37.1061",
      "com.microsoft.sqlserver" % "mssql-jdbc" % "7.2.1.jre8"
    ),

    unmanagedBase := baseDirectory.value / "../../libraries/"

  )