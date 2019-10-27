import Dependencies._

name := "kafka-streams-thomas"

version := "0.1"

scalaVersion := "2.12.10"

scalacOptions in ThisBuild := Seq("-Xexperimental", "-Xlint:_", "-unchecked", "-deprecation", "-feature", "-target:jvm-1.8")

lazy val client = (project in file("./client"))
  .settings(libraryDependencies ++= Seq(kafka, curator))
  .dependsOn(protobufs, configuration)

lazy val configuration = project in file("./configuration")

lazy val protobufs = (project in file("./protobufs"))
  .settings(
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    )
  )

lazy val model = (project in file("./model"))
  .settings(libraryDependencies ++= Dependencies.modelsDependencies)
  .dependsOn(protobufs)