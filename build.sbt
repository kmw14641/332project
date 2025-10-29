ThisBuild / scalaVersion := "2.13.12"

lazy val common = (project in file("common"))

lazy val master = (project in file("master"))
  .dependsOn(common)

lazy val worker = (project in file("worker"))
  .dependsOn(common)

lazy val root = (project in file("."))
  .aggregate(common, master, worker)