import sbt._

object Dependencies {

  val scalaV = "2.12.10"

  val sparkV = "3.5.0"
  val hadoopAwsV = "3.3.4"
  val awsJavaSdkV = "1.11.1033"
  val sparkRedshiftV = "6.2.0-spark_3.5"
  val redshiftV = "2.1.0.24"
  val deltaV = "3.0.0"

  val jacksonV = "2.18.0"
  val circeV = "0.13.0"
  val configV = "1.3.2"
  val scalaTestV = "3.1.2"

  // This section contains the dependency override for this whole project
  val depsOverrides = Seq(
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonV,
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonV,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonV,
    "io.netty" % "netty-all" % "4.1.110.Final",
    "org.apache.hadoop" % "hadoop-client-api" % hadoopAwsV % Provided,
  )

  // This section contains common dependencies which is required by all the sub modules in this project
  private val sparkCoreDependencies = Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-sql" % sparkV,
    "org.apache.spark" %% "spark-catalyst" % sparkV,
    "org.apache.spark" %% "spark-hive" % sparkV,
    "io.delta" %% "delta-spark" % deltaV,
  ).map(_ % Provided)

  private val loggingDependencies = Seq(
    "org.slf4j" % "slf4j-api" % "1.7.26",
    "ch.qos.reload4j" % "reload4j" % "1.2.22" exclude("org.slf4j", "log4j-over-slf4j"),
    "ch.qos.logback" % "logback-classic" % "1.2.3"
  )

  private val testSparkDependencies = Seq(
    "org.apache.spark" %% "spark-core" % sparkV,
    "org.apache.spark" %% "spark-sql" % sparkV,
    "org.apache.spark" %% "spark-catalyst" % sparkV
  ).map(_ % Test classifier "tests")

  private val testDependencies = Seq(
    "org.scalatest" %% "scalatest" % scalaTestV,
    "org.scalatestplus" %% "mockito-3-4" % "3.3.0.0-SNAP3",
    "org.scalactic" %% "scalactic" % scalaTestV,
    "org.scalamock" %% "scalamock" % "4.4.0",
    "org.mockito" % "mockito-core" % "3.11.2",
    "com.h2database" % "h2" % "2.2.224",
    "com.github.mrpowers" %% "spark-fast-tests" % "1.0.0" // Alternative could be https://github.com/holdenk/spark-testing-base which is more actively maintained
  ).map(_ % Test) ++ testSparkDependencies

  val baseDependencies = sparkCoreDependencies ++ loggingDependencies ++ testDependencies

  /*
   This section contains dependencies which is required by `core` modules,
   */
  val coreModuleDependencies = Seq(
    "com.thoughtworks.paranamer" % "paranamer" % "2.8",

    // ADLS Dependencies
    "org.apache.hadoop" % "hadoop-azure" % "3.3.4" exclude("org.apache.hadoop.thirdparty", "hadoop-shaded-guava"),
//    "org.apache.hadoop" % "hadoop-hdfs"   % "3.3.4",
    "org.apache.hadoop" % "hadoop-azure-datalake" % "3.3.4"  exclude("org.apache.hadoop.thirdparty", "hadoop-shaded-guava"),
    "com.azure" % "azure-identity" % "1.11.0",
    "com.azure" % "azure-core-http-netty" % "1.15.0",
    "com.azure" % "azure-storage-blob" % "12.30.0",
    "com.azure" % "azure-storage-file-datalake" % "12.18.0",
    "com.azure" % "azure-security-keyvault-secrets" % "4.10.2",
    "com.microsoft.sqlserver" % "mssql-jdbc" % "12.2.0.jre11" % Provided,

    // AWS Dependencies
    "com.amazonaws" % "aws-java-sdk" % awsJavaSdkV % Provided,
    "software.amazon.awssdk" % "secretsmanager" % "2.16.23" % Provided,
    "com.amazonaws.secretsmanager" % "aws-secretsmanager-jdbc" % "1.0.12" % Provided,
    "org.apache.hadoop" % "hadoop-aws" % hadoopAwsV % Provided excludeAll(
      ExclusionRule(organization = "com.amazonaws"),
      ExclusionRule(organization = "javax.ws.rs"),
      ExclusionRule(organization = "io.netty"),
      ExclusionRule(organization = "org.apache.avro"),
      ExclusionRule(organization = "javax.servlet", name = "servlet-api"),
      ExclusionRule(organization = "javax.servlet.jsp", name = "jsp-api"),
      ExclusionRule(organization = "org.mortbay.jetty", name = "servlet-api")
    ),
    "org.apache.hadoop" % "hadoop-hdfs" % hadoopAwsV % Provided excludeAll(
      ExclusionRule(organization = "com.amazonaws"),
      ExclusionRule(organization = "javax.ws.rs"),
      ExclusionRule(organization = "io.netty"),
      ExclusionRule(organization = "org.apache.avro")
    ),
    "org.apache.hadoop" % "hadoop-common" % hadoopAwsV % Provided excludeAll(
      ExclusionRule(organization = "com.amazonaws"),
      ExclusionRule(organization = "javax.ws.rs"),
      ExclusionRule(organization = "io.netty"),
      ExclusionRule(organization = "org.apache.avro")
    ),

    // Monitoring Dependencies
    "com.paulgoldbaum" %% "scala-influxdb-client" % "0.6.1",

    // JSON Parser Dependencies
    "io.circe" %% "circe-parser" % circeV,
    "io.circe" %% "circe-core" % circeV,
    "io.circe" %% "circe-generic" % circeV,

    // Config Dependencies
    "com.typesafe" % "config" % configV,
    "com.github.pureconfig" %% "pureconfig" % "0.9.1",

    "com.launchdarkly" % "launchdarkly-java-server-sdk" % "7.8.0"
  )

  /*
   This section contains dependencies which is required by `batch-events` modules,
   */
  val batchEventsDependencies = Seq(
    // Redshift Dependencies
    "com.lihaoyi" %% "requests" % "0.8.0",
    "com.amazon.redshift" % "redshift-jdbc42" % redshiftV % Provided,
    "io.github.spark-redshift-community" %% "spark-redshift" % sparkRedshiftV % Provided excludeAll(
      ExclusionRule(organization = "org.apache.avro"),
      ExclusionRule(organization = "com.amazonaws")
    ),



//     Other Dependencies
    "org.rauschig" % "jarchivelib" % "0.8.0",
    "org.scalikejdbc" %% "scalikejdbc" % "3.2.2",
    "org.apache.spark" %% "spark-avro" % sparkV % Provided
  )

  /*
   This section contains dependencies which is required by spark-streaming related modules,
   i.e. `ccl-streaming-events` and `streaming-events`
   */
  val streamingDependencies = Seq(
    "org.apache.spark" %% "spark-streaming" % sparkV % Provided,
    "org.apache.kafka" % "kafka-clients" % "1.0.1" excludeAll ExclusionRule(organization = "net.jpountz.lz4", name = "lz4"),
    "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkV
  )
}
