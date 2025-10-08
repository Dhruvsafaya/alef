package com.alefeducation.transformer

import com.alefeducation.base.SparkBatchService
import com.alefeducation.io.UnityCatalog
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources._
import com.alefeducation.util.{AwsUtils, BatchExecutionLogger, Offset, OffsetLogger, SparkSessionUtils}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

abstract class BaseTransform(val serviceName: String) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

  private val sessionName: String = this.getClass.getName
  protected val spark: SparkSession = SparkSessionUtils.getSession(sessionName)
  protected val service = new SparkBatchService(serviceName, spark)
  protected val unity: UnityCatalog = new UnityCatalog(spark)
  protected val batchExecutionLogger = new BatchExecutionLogger(spark, unity)
  protected val offsetLogger = new OffsetLogger

  protected val batchJobId: String = getString(serviceName, "batch-job-id")
  val sources: List[String] = getSourceList(serviceName)
  protected val sinkName: String = getNestedString(serviceName, "sink")
  protected val silverTableName: String = getOptionString(serviceName, "silver-table-name").getOrElse("")

  def transform(data: Map[String, Option[DataFrame]], startId: Long): Option[DataFrame]

  private def execute(data: Option[DataFrame], executables: List[ExecutableV2], key: String): Unit = {

    executables.foreach { executable =>
      batchExecutionLogger.log("STARTED", batchJobId, executable.source, executable.currentOffset)
    }

    val hasMoved = executables.exists(ex => ex.currentOffset.hasMoved(ex.previousOffset))

    Try {
      if (hasMoved) {
        val sink = data.map(DataSink(sinkName = sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> key)))
        service.run(sink)
      }
    } match {
      case Success(_) => {
        executables.foreach { executable =>
          val status = if (executable.currentOffset.hasMoved(executable.previousOffset)) "SUCCESS" else "SKIPPED"
          batchExecutionLogger.log(status, batchJobId, executable.source, executable.currentOffset)
        }
        if(silverTableName.nonEmpty) {
          val offset: String = convertToJsonString(executables.map(e => e.source -> e.currentOffset.end).toMap)
          offsetLogger.log(offset, getOffsetLogS3Path, silverTableName)
        }
      }
      case Failure(exception) => {
        logger.error("Error processing batch job", exception)
        executables.foreach { executable =>
          batchExecutionLogger.log("FAILED", batchJobId, executable.source, executable.currentOffset)
        }
        throw exception
      }
    }
  }

  private def convertToJsonString(valueMap: Map[String, Long]): String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(valueMap)
  }

  /**
   * Read the previous offset and read the bronze layer after the offset
   * Remove pure duplicates by removing generated columns before send for transformation
   */
  def run(): Unit = {
    val startId: Long = getStartId
    val executables = sources.map { source =>
      val previousOffset = batchExecutionLogger.getPreviousOffset(batchJobId, source)
      val generatedColumns = Set("_load_time", "_trace_id", "dw_id", "_commit_version", "_commit_timestamp")
      val sourceDF: Option[Dataset[Row]] = unity.readOptional(getBronzeTablePathFor(source))
        .flatMap(df => df.where(col("_load_time") > previousOffset.end)
          .dropDuplicates(df.columns.toSet.diff(generatedColumns).toSeq)
          .checkEmptyDf
        )
      val currentOffset = sourceDF.map(sdf => batchExecutionLogger.getOffsets(sdf).getOrElse(previousOffset)).getOrElse(previousOffset)
      ExecutableV2(source, sourceDF, previousOffset, currentOffset)
    }
    val data = executables
      .map { ex =>
        ex.source -> ex.df
      }
      .filter(a => a._2.isDefined)
      .toMap

    val transformedDF = transform(data, startId)
    execute(transformedDF, executables, batchJobId)
  }

  private def getStartId = {
    service.prehook
    val startId = service.getStartIdUpdateStatus(batchJobId)
    startId
  }

  private def getSourceList(serviceName: String): List[String] = {
    val sources = getList(serviceName, "sources")
    val source = getOptionString(serviceName, "source").toList
    source ++ sources
  }
}

case class ExecutableV2(source: String, df: Option[DataFrame], previousOffset: Offset, currentOffset: Offset)
