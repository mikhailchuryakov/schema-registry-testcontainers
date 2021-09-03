name := "schema-registry-testcontainers"

version := "0.1"

scalaVersion := "2.13.6"

scalacOptions ++= Seq(
  "-deprecation",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-unchecked",
  "-feature",
  "-Ymacro-annotations",
  "-language:higherKinds",
)

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

libraryDependencies += "com.dimafeng" %% "testcontainers-scala-kafka" % "0.39.7"
libraryDependencies += "com.github.fd4s" %% "fs2-kafka-vulcan" % "1.7.0"
libraryDependencies += "com.sksamuel.avro4s" %% "avro4s-kafka" % "4.0.9"

libraryDependencies += "org.typelevel" %% "cats-core" % "2.6.1"
libraryDependencies +="org.typelevel" %% "cats-effect" % "2.5.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9"

