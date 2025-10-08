package com.alefeducation.util

import com.alefeducation.schema.Schema._
import com.paulgoldbaum.influxdbclient._
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Level.INFO
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{explode, from_json, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pureconfig._
import pureconfig.error.{ConfigReaderFailure, ConfigReaderFailures}

import java.util
import scala.concurrent.Await
import java.util.concurrent.TimeUnit._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

object Resources {

  val logger = Logger.getLogger(classOf[Resource])

  val conf = ConfigFactory.load()

  def loadMap(namespace: String): Map[String, String] =
    loadConfigOrThrow[Map[String, String]](namespace)

  def getResource(name: String, path: String): Set[Resource] = {
    import collection.JavaConverters._
    val resources = conf.getConfig(name).getStringList(path).asScala.toSet
    logger.log(INFO, s"Resources loaded: $resources")
    resources.map(name => Resource(name, loadMap(name)))
  }

  def getResourceByName(name: String): Either[Throwable, Resource] = {
    val ei = loadConfig[Map[String, String]](name).left.map(cfs => new RuntimeException(configReaderFailureToString(cfs)))
    val resource = ei.map(Resource(name, _))
    logger.log(INFO, s"Resources loaded: $resource")
    resource
  }

  def configReaderFailureToString(cfs: ConfigReaderFailures): String = {
    cfs.toList.map(_.description).mkString(", ")
  }

  case class Resource(name: String, props: Map[String, String])

  def getPayloadStream(session: SparkSession, dataFrames: DataFrame, useKafkaHeader: Boolean = false): DataFrame = {
    import session.implicits._

    if (useKafkaHeader) {
      //extract eventType column from header
      dataFrames
        .select($"value".cast("string"), explode($"headers").as("headerItem"), $"timestamp".as("loadtime"), $"headers".as("_headers"))
        .filter($"headerItem.key" === "eventType")
        .select(
          regexp_replace($"headerItem.value".cast("string"), "\"", "").as("eventType"),
          $"value".as("body"),
          $"loadtime",
          $"_headers"
        )

    } else {
      dataFrames
        .select(from_json($"value".cast("string"), kafkaSchema).as("message"), $"timestamp".as("loadtime"))
        .select(
          $"message.headers.*",
          $"message.body".as("body"),
          $"loadtime"
        )
    }
  }

  def env(): String = conf.getString("env")

  def executionEnv(): String = conf.getString("execution_env")

  def localS3Endpoint(): String = conf.getString("s3_endpoint")

  def redshiftSchema(): String = conf.getString("fabric-schema")

  def redshiftStageSchema(): String = conf.getString("fabric-staging-schema")

  def mode(): String = Try(System.getenv("mode")).getOrElse("live")

  def getString(name: String): String = conf.getString(name)

  def getLong(name: String): Long = conf.getLong(name)

  def getS3Root(): String = conf.getString("s3.root")

  def getS3Path(): String = conf.getString("s3.path")

  def getS3DataLakeRoot(): String = conf.getString("s3.datalake")

  def getCoalesceValue(): Int = conf.getInt("s3.coalesce")

  def getS3DeltaPath(): String = conf.getString("s3.delta.path")

  def isDeltaEnabled(): Boolean = conf.getBoolean("s3.delta.enable")

  def getDeltaSnapshotPartitions(): Int = conf.getInt("delta.snapshot.partitions")

  def getSparkShufflePartitions(): Long = conf.getLong("spark.shuffle.partitions")

  def getNestedString(field: String, nestedField: String): String = conf.getString(s"$field.$nestedField")

  def hasNestedPath(field: String, nestedField: String): Boolean = conf.hasPath(s"$field.$nestedField")

  def getNestedMap(field: String, nestedField: String): Map[String, String] =
    if (conf.hasPath(s"$field.$nestedField")) {
      conf.getConfig(s"$field.$nestedField").entrySet().asScala.map(e => e.getKey -> e.getValue.unwrapped().toString).toMap
    } else {
      Map.empty
    }

  @deprecated("use getNestedMap")
  def getOptionalMap(field: String, nestedField: String): Option[Map[String, String]] =
    Try(conf.getConfig(s"$field.$nestedField").entrySet().asScala.map(e => e.getKey -> e.getValue.unwrapped().toString).toMap).toOption

  def getNestedConfig(field: String, nestedField: String): Map[String, AnyRef] =
    conf.getConfig(s"$field.$nestedField").entrySet().asScala.map(e => e.getKey -> e.getValue.unwrapped()).toMap

  def getSource(name: String): List[String] = conf.getStringList(s"$name.source").asScala.toList

  def getSink(name: String): List[String] = conf.getStringList(s"$name.sink").asScala.toList

  def getUniqueIds(name: String): List[String] = conf.getStringList(s"$name.unique-ids").asScala.toList

  def getMaxIdsTablePath(): String = conf.getString("delta-max-ids")

  def getDataOffsetPath(): String = conf.getString("delta-data-offset")

  def getSqlExceptionTimeout(): Int = conf.getInt("redshift.sqlexceptiontimeout")

  def getSqlExceptionAttempts(): Int = conf.getInt("redshift.sqlexceptionattempts")

  def getBool(field: String, nestedField: String): Boolean = Try{conf.getBoolean(s"$field.$nestedField")}.getOrElse(false)

  def getNestedStringIfPresent(field: String, nestedField: String): Option[String] = {
    if (hasNestedPath(field, nestedField)) {
      Some(getNestedString(field, nestedField))
    } else None
  }

  def getList(field: String, nestedField: String): List[String] =
    Try(conf.getStringList(s"$field.$nestedField").asScala.toList).getOrElse(List.empty)

  def getString(field: String, nestedField: String): String = conf.getString(s"$field.$nestedField")

  def getOptionString(field: String, nestedField: String): Option[String] = Try(conf.getString(s"$field.$nestedField")).toOption

  def getInt(field: String, nestedField: String): Int = conf.getInt(s"$field.$nestedField")

  def getNestedMapList(field: String, nestedField: String): List[Map[String, String]] = {
    // Fetch the list of nested configurations
    val configList = conf.getConfigList(s"$field.$nestedField").asScala

    // Convert each nested config to a Map[String, String]
    configList.map { config =>
      config.entrySet().asScala.map(e => e.getKey -> e.getValue.unwrapped().toString).toMap
    }.toList
  }

  def getConfigList(field: String, nestedField: String): List[Config] = {
    // Fetch the list of nested configurations
    Try(conf.getConfigList(s"$field.$nestedField").asScala.toList).getOrElse(List.empty[Config])
  }

  def getBronzeTableNameFor(source: String): String = env() + ".bronze." + source

  def getBronzeTablePathFor(source: String): String =  getS3Path() + "/" + env() + "/catalog/bronze/data/" + source

  def getBatchExecutionLogTable: String = conf.getConfig("internal").getString("batch.execution.log.table")

  def getBatchExecutionLogS3Path: String = getS3Path() + "/" + env() + "/catalog/_internal/batch_execution_log/"

  def getOffsetLogS3Path: String = getS3Root() + "/" + env() + "/catalog/_internal/offset_log/"

  lazy val ALEF_39389_PATHWAY_CONTENT_REPO_IWH_TOGGLE: Boolean = conf.getBoolean("toggle.pathway.content.repo.iwh")
}
