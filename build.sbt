name := "SProject"

version := "0.1"

scalaVersion := "2.10.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.2",
  "org.apache.spark" %% "spark-sql" % "1.6.2",
  "org.apache.spark" %% "spark-mllib" % "1.6.2",
  "mysql" % "mysql-connector-java" % "5.1.12",
  "org.apache.commons" % "commons-csv" % "1.5",
  "com.databricks" % "spark-csv_2.10" % "1.5.0"
)