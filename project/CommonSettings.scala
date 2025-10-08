import sbt.Keys._
import sbt._
import Dependencies._
import sbtassembly.AssemblyPlugin.autoImport._

object CommonSettings {

  private val assemblySettings = Seq(
    assemblyMergeStrategy in assembly := {
      case PathList("application.conf") => MergeStrategy.concat
      case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
      case PathList("javax", "inject", xs@_*) => MergeStrategy.last
      case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
      case PathList("javax", "activation", xs@_*) => MergeStrategy.last
      case PathList("org", "apache", xs@_*) => MergeStrategy.last
      case PathList("com", "google", xs@_*) => MergeStrategy.last
      case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
      case PathList("com", "codahale", xs@_*) => MergeStrategy.last
      case PathList("com", "yammer", xs@_*) => MergeStrategy.last
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.discard
      case PathList("META-INF", "versions", _ @ _*) => MergeStrategy.discard
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case PathList("org", "objectweb", _ @ _*) => MergeStrategy.last
      case PathList("io", "netty", _ @ _*) => MergeStrategy.last
      case x if x.contains("org/objectweb") => MergeStrategy.last
      case x if x.contains("io/netty") => MergeStrategy.last
      case x if x.contains("io.netty.versions.properties") ⇒ MergeStrategy.last
      case "about.html" => MergeStrategy.rename
      case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
      case "META-INF/mailcap" => MergeStrategy.last
      case "META-INF/mimetypes.default" => MergeStrategy.last
      case "plugin.properties" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    },
    artifact in(Compile, assembly) := {
      val art = (artifact in(Compile, assembly)).value
      art.withClassifier(`classifier` = Some("assembly"))
    },
    assemblyShadeRules in assembly := Seq(ShadeRule.rename("okio.**" -> "okio-shade.@1").inAll),
    assemblyMergeStrategy in assembly := {
      case PathList("application.conf") => MergeStrategy.concat
      case PathList("META-INF", "services", "software.amazon.awssdk.http.SdkHttpService") => MergeStrategy.filterDistinctLines
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat

      // discard signatures
      case PathList("META-INF", "ECLIPSEF.RSA") => MergeStrategy.discard
      case PathList("META-INF", "ECLIPSEF.SF")  => MergeStrategy.discard
      case PathList("META-INF", "ECLIPSEF.DSA") => MergeStrategy.discard

      // discard other META-INF junk
      case PathList("META-INF", xs @ _*) if xs.nonEmpty => MergeStrategy.discard
      case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.discard
      case PathList("META-INF", "versions", _ @ _*) => MergeStrategy.discard
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case PathList("webapps", _*) => MergeStrategy.discard
      case "jquery.slim.min.js" => MergeStrategy.discard
      case "jquery.min.js" => MergeStrategy.discard
      case "module-info.class" => MergeStrategy.discard
      case "scala/tools/nsc/doc/html/resource/lib/jquery.mousewheel.min.js" => MergeStrategy.discard
      case "scala/tools/nsc/doc/html/resource/lib/jquery.panzoom.min.js" => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    assemblyExcludedJars in assembly := {
      val classPath = (fullClasspath in assembly).value
      classPath.filter { file =>
        val name = file.data.getName.toLowerCase
        name.contains("log4j") || name.contains("meta-inf") || name.contains("versions.properties") || name.contains("module-info.class")
      }
    }
  )

  private val publishSettings = Seq(
    publishLocalConfiguration := publishLocalConfiguration.value.withOverwrite(true),
    publishConfiguration := publishConfiguration.value.withOverwrite(true),
    publishArtifact := true,
    publishArtifact in Test := false
  )

  val commonAssemblySettings = Seq(
    test in assembly := {}
  ) ++ assemblySettings ++ publishSettings ++ addArtifact(artifact in(Compile, assembly), assembly)
}
