addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.3.1")

// https://scalapb.github.io/docs/installation/
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.0.6")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.11"