ThisBuild / scalaVersion := "2.13.17"

val commonSettings = Seq(
  // https://scalapb.github.io/docs/installation/, https://scalapb.github.io/docs/grpc
  Compile / PB.targets := Seq(
    scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
  ),
  libraryDependencies ++= Seq(
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion
  )
)

lazy val common = (project in file("common"))
  .settings(commonSettings)

lazy val master = (project in file("master"))
  .settings(commonSettings)
  .settings(
    assemblyJarName := "master.jar",
  )
  .dependsOn(common)

lazy val worker = (project in file("worker"))
  .settings(commonSettings)
  .settings(
    assemblyJarName := "worker.jar",
  )
  .dependsOn(common)

lazy val root = (project in file("."))
  .aggregate(common, master, worker)