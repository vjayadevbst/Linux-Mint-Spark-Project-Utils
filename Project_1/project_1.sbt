name := "Project_1"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0"
)

lazy val root = (project in file(".")).dependsOn(utils)

lazy val utils = RootProject(file("../utils"))
