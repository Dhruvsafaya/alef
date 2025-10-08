package com.alefeducation.util

import com.alefeducation.util.Resources.{env, getDeltaSnapshotPartitions, getSparkShufflePartitions, mode}
import com.alefeducation.util.DefaultAzureTokenProvider
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{lit, to_json}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util.TimeZone

object SparkSessionUtils {

  def getSession(name: String): SparkSession = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    System.setProperty("user.timezone", "UTC")

    val conf = new SparkConf()
    if (mode() == "debug") conf.set("spark.master", "local[2]")
    conf.set("spark.app.name", s"${env() + "-" + name}")
    conf.set("spark.metrics.namespace", s"${env() + "-" + name}")
    conf.set("spark.sql.session.timeZone", "UTC")
    conf.set("spark.databricks.delta.snapshotPartitions", String.valueOf(getDeltaSnapshotPartitions()))
    conf.set("spark.sql.shuffle.partitions", String.valueOf(getSparkShufflePartitions()))
    conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600") // setting 100 MB limit on broadcast hash join
    conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    conf.set("spark.sql.datetime.java8API.enabled", "false")
    conf.set("spark.sql.debug.maxToStringFields", "100")
    conf.set("spark.sql.legacy.typeCoercion.datetimeToString.enabled", "true")
    conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    conf.set("fs.s3a.endpoint", sys.env.getOrElse("S3_ENDPOINT", ""))
    conf.set("fs.s3a.path.style.access", sys.env.getOrElse("S3_PATH_STYLE_ACCESS", "false"))
    conf.set("fs.s3a.connection.ssl.enabled", sys.env.getOrElse("S3_SSL_ENABLED", "true"))
    conf.set("fs.azure.account.auth.type.alefdatapoc.dfs.core.windows.net", "Custom")
    conf.set("fs.azure.account.oauth.provider.type.alefdatapoc.dfs.core.windows.net", "com.alefeducation.util.DefaultAzureTokenProvider")
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    val session = SparkSession.builder().config(conf).getOrCreate()
    session.sparkContext.setLogLevel("INFO")
    session
  }

  def getJsonData(session: SparkSession, config: String): Option[DataFrame] = {
    import session.implicits._

    val data = session.conf.get(config, "")
    val jsonDataSchema = session.conf.get(s"${config}Schema", "")

    config match {
      case conf if conf.startsWith("csv") =>
        val csvData: Dataset[String] = data.stripMargin.linesIterator.toList.toDS()
        Some(session.read.option("header", "true").option("mode", "FAILFAST").option("inferSchema", true).csv(csvData))
      case _ =>
        if (data.isEmpty) None
        else {
          import session.implicits._
          if (jsonDataSchema.isEmpty) {
            Some(session.read.option("multiline", "true").option("inferTimestamp", "false").json(Seq(data).toDS))
          } else {
            Some(session.read.option("multiline", "true").option("inferTimestamp", "false").schema(jsonDataSchema).json(Seq(data).toDS))
          }
        }
    }
  }

  def getKafkaDataFrame(session: SparkSession, config: String): DataFrame = {
    import session.implicits._

    val headerSchema = ArrayType(StructType(Seq(StructField("key", StringType), StructField("value", BinaryType))))

    val jsonDf = getJsonData(session, config).getOrElse(session.emptyDataFrame).withColumn("value", to_json($"value").cast(StringType))
    if (jsonDf.columns.contains("headers")) {
      //process kafka headers provided in a test
      jsonDf.withColumn("headers", $"headers".cast(headerSchema))
    }
    else {
      //by default set kafka header as null
      jsonDf.withColumn("headers", lit(null).cast(headerSchema))
    }
  }
}
