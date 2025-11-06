// Import sbt-release plugin
import sbtrelease.ReleaseStateTransformations._
import sbtrelease.ReleasePlugin
import sbt._
import sbt.Keys._

enablePlugins(ReleasePlugin)

val scala_2_12 = "2.12.18"
val scala_2_13 = "2.13.12"
val scalaversions = Seq(scala_2_12,scala_2_13)
val sparkVersion = "3.3.2"
val requestVersion = "0.8.3"
val upickleVersion = "4.0.1"


crossScalaVersions := scalaversions
scalaVersion := "2.13.12"

// Package Owner info
name := "odata-spark-connector"
organization := "io.github.narramukhesh"
scmInfo := Some(ScmInfo(url("https://github.com/narramukhesh/projectone"), "git@github.com:narramukhesh/projectone.git"))
homepage:= Some(url("https://github.com/narramukhesh/projectone"))
developers:= List(Developer("mukhesh narra", "mukhesh narra", "narramukhesh@gmail.com", url("https://github.com/narramukhesh")))
licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

// Library Dependencies
libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.scala-lang.modules" %% "scala-parser-combinators" % "2.3.0",
      "com.lihaoyi" %% "requests" % requestVersion,
      "com.lihaoyi" %% "upickle" % upickleVersion,
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "org.scalatestplus" %% "mockito-4-11" % "3.2.18.0" % "test"
    )


// Release settings
publishMavenStyle := true
releaseVersionFile := file("version.sbt")
releaseCrossBuild := true  // Enable cross-building during the release process
releaseVersionBump := sbtrelease.Version.Bump.NextStable

ThisBuild / publishTo := {
  val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
  if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
  else localStaging.value
}

ThisBuild / versionScheme := Some("semver-spec")
releaseTagComment        := s"chore: (connectors)(spark)(odata) Releasing ${(ThisBuild / version).value} using sbt-release"
releaseCommitMessage     := s"chore: (connectors)(spark)(odata) Setting version to ${(ThisBuild / version).value} using sbt-release"
releaseNextCommitMessage := s"chore: (connectors)(spark)(odata) Setting version to ${(ThisBuild / version).value} using sbt-release"

releaseNextVersion := (releaseVersion => releaseVersion.split("\\.") match {
  case Array(major, minor, bugfix) =>
    s"$major.$minor.${bugfix.toInt + 1}"
})

releaseIgnoreUntrackedFiles := true

// Define the release process
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,  // Check for snapshot dependencies
  inquireVersions,             // Automatically ask for versions to release
  setReleaseVersion,           // Set the release version
  commitReleaseVersion,        // Commit version changes
  tagRelease,                  // Tag the release
  releaseStepCommandAndRemaining("+publishSigned"), // publish the signed artifacts to the sonatype staging repository
  setNextVersion,             // set next version by the release process
  commitNextVersion,        // Commit version changes
  releaseStepCommand("sonatypeBundleRelease"), // release to the central 
  pushChanges                  // Push changes to version control
)

// Publish artifacts

publishConfiguration := publishConfiguration.value.withOverwrite(true)
publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true)