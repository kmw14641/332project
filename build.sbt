import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy
import sbtassembly.PathList

// ===========================================================
// Common Merge Strategy (works for Netty, protobuf, gRPC, etc.)
// Merge strategies define how to handle conflicts when combining files
// from different JARs into a single JAR during the assembly process.
// Without this merge strategy, the assembly task & executing jar file will fail.
// ===========================================================
lazy val customMergeStrategy: String => MergeStrategy = {
  case PathList("META-INF", "services", xs @ _*) =>
    MergeStrategy.concat

  case PathList("META-INF", xs @ _*) => xs match {
    case ("MANIFEST.MF") :: Nil => MergeStrategy.discard
    case _ => MergeStrategy.discard
  }

  case PathList("reference.conf") => MergeStrategy.concat
  case PathList("application.conf") => MergeStrategy.concat
  case "module-info.class" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

ThisBuild / scalaVersion := "2.13.17"
ThisBuild / scalacOptions ++= Seq(
  "-Xasync"
)

val commonSettings = Seq(
  // https://scalapb.github.io/docs/installation/, https://scalapb.github.io/docs/grpc
  Compile / PB.targets := Seq(
    scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
  ),
  libraryDependencies ++= Seq(
    "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion,
    "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
    "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
    "com.github.scopt" %% "scopt" % "4.1.0",
    "org.scala-lang.modules" %% "scala-async" % "1.0.1",
  )
)

lazy val common = (project in file("common"))
  .settings(commonSettings)

lazy val master = (project in file("master"))
  .settings(commonSettings)
  .settings(
    assemblyJarName := "master.jar",
    assemblyMergeStrategy := customMergeStrategy
  )
  .dependsOn(common)

lazy val worker = (project in file("worker"))
  .settings(commonSettings)
  .settings(
    assemblyJarName := "worker.jar",
    assemblyMergeStrategy := customMergeStrategy
  )
  .dependsOn(common)

lazy val root = (project in file("."))
  .aggregate(common, master, worker)
  .settings(assemblyMergeStrategy := customMergeStrategy)
