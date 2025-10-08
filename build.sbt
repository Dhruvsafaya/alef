import Dependencies._
import sbt.Keys._
import CommonSettings._

//ThisBuild / credentials += Credentials("Sonatype Nexus Repository Manager","nexus-repo.alefed.com", sys.env("NEXUS_TOKEN_USR") , sys.env("NEXUS_TOKEN_PSW"))
ThisBuild / scalaVersion := scalaV
ThisBuild / organization := "com.alefeducation"
ThisBuild / libraryDependencies := baseDependencies ++ coreModuleDependencies ++ batchEventsDependencies ++ streamingDependencies
ThisBuild / dependencyOverrides := depsOverrides
ThisBuild / version      := "0.2.0-SNAPSHOT"
//ThisBuild / resolvers := Seq(
//  Resolver.defaultLocal,
//  "Maven2-Proxy" at "https://nexus-repo.alefed.com/repository/Maven2-Proxy/",
//  "maven-aws-redshift" at "https://nexus-repo.alefed.com/repository/maven-aws-redshift/",
//  "ivy-release" at "https://nexus-repo.alefed.com/repository/ivy-releases/",
//  "SBT Plugin Release" at "https://nexus-repo.alefed.com/repository/ivy-scala-sbt/"
//)

ThisBuild / parallelExecution in Test := false
ThisBuild / testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
ThisBuild / logBuffered := false

val printBatchTests = taskKey[Unit]("Print full class names of tests to the file `test-full-class-names.log`.")

printBatchTests := {
  import java.io._
  println("Print full class names of tests to the file `test-full-class-names.log`.")
  val pw = new PrintWriter(new File("test-full-class-names.log"))
  (definedTests in Test in `batch-events`).value.foreach { t =>
    pw.println(t.name)
  }
  pw.close()
}

//val jacocoReportFormats = Seq()

lazy val root = project
  .in(file("."))
  .settings(
    name := "alef-data-platform",
    fork in Test := true,
//    fork in jacoco := true,
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled"),
    publishArtifact := false
  )
  .disablePlugins(AssemblyPlugin)
  .aggregate(
    core,
    `core-versioned`,
    `batch-events`,
    `batch-events-versioned`,
    `streaming-events`,
    `streaming-events-versioned`,
    `ccl-streaming-events`,
    `ccl-streaming-events-versioned`
  ).settings(
//  jacocoAggregateReportSettings := JacocoReportSettings(
//    title = "Alef Data Platform Coverage Report",
//    formats = jacocoReportFormats
//  )
)
//change the threshold back after producionizing service desk
lazy val core = project.in(file("core"))
  .settings(
    commonAssemblySettings,
//    jacocoReportSettings := JacocoReportSettings(formats = jacocoReportFormats)
//      .withThresholds(
//        JacocoThresholds(
//          line = 14,
//          instruction = 6.91,
//          branch = 0.46,
//          method = 4,
//          complexity = 2,
//          clazz = 23.03
//        )
//      )
  )


lazy val `core-versioned` = project
  .in(file("core/build/versioned"))
  .enablePlugins(JavaAppPackaging)
  .settings(
    commonAssemblySettings,
    version := "1.0.0",
    publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true),
    publishConfiguration := publishConfiguration.value.withOverwrite(true)
  )
  .dependsOn(core)
lazy val `batch-events` = project
  .in(file("batch-events"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    commonAssemblySettings,
//    jacocoReportSettings := JacocoReportSettings(formats = jacocoReportFormats)
//      .withThresholds(
//        JacocoThresholds(
//          line = 74,
//          instruction = 56,
//          branch = 10,
//          method = 28,
//          complexity = 23,
//          clazz = 44
//        )
//      ),
//    jacocoExcludes := Seq("com.alefeducation.models.*")
  )

lazy val `batch-events-versioned` = project
  .in(file("batch-events/build/versioned"))
  .enablePlugins(JavaAppPackaging)
  .settings(
    commonAssemblySettings,
    version := "1.0.0",
    publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true),
    publishConfiguration := publishConfiguration.value.withOverwrite(true)
  )
  .dependsOn(`batch-events`)

lazy val `streaming-events` = project
  .in(file("streaming-events"))
  .dependsOn(core% "compile->compile;test->test")
  .settings(
    commonAssemblySettings,
//    jacocoReportSettings := JacocoReportSettings(formats = jacocoReportFormats)
//      .withThresholds(
//        JacocoThresholds(
//          line = 59,
//          instruction = 57,
//          branch = 0,
//          method = 31,
//          complexity = 27,
//          clazz = 51
//        )
//      )
  )

lazy val `streaming-events-versioned` = project
  .in(file("streaming-events/build/versioned"))
  .enablePlugins(JavaAppPackaging)
  .settings(
    commonAssemblySettings,
    version := "1.0.0",
    publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true),
    publishConfiguration := publishConfiguration.value.withOverwrite(true)
  )
  .dependsOn(`streaming-events`)

lazy val `ccl-streaming-events` = project
  .in(file("ccl-streaming-events"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    commonAssemblySettings,
//    jacocoReportSettings := JacocoReportSettings(formats = jacocoReportFormats)
//      .withThresholds(
//        JacocoThresholds(
//          line = 98,
//          instruction = 96,
//          branch = 50,
//          method = 60,
//          complexity = 54,
//          clazz = 100
//        )
//      )
  )

lazy val `ccl-streaming-events-versioned` = project
  .in(file("ccl-streaming-events/build/versioned"))
  .enablePlugins(JavaAppPackaging)
  .settings(
    commonAssemblySettings,
    version := "1.0.0",
    publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true),
    publishConfiguration := publishConfiguration.value.withOverwrite(true)
  )
  .dependsOn(`ccl-streaming-events`)
