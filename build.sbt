ThisBuild / scalaVersion := "2.13.17"

lazy val common = (project in file("common"))

lazy val master = (project in file("master"))
  .settings(
    assemblyJarName := "master.jar",
  )
  .dependsOn(common)

lazy val worker = (project in file("worker"))
  .settings(
    assemblyJarName := "worker.jar",
  )
  .dependsOn(common)

lazy val root = (project in file("."))
  .aggregate(common, master, worker)