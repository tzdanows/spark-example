ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.9"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark examples"
  )
