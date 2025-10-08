package com.alefeducation.transformer

import com.alefeducation.base.SparkBatchService
import com.alefeducation.io.UnityCatalog
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{getBronzeTableNameFor, getSink}
import com.alefeducation.util.{BatchExecutionLogger, Offset, SparkSessionUtils}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

abstract class DeprecatedBaseTransform {
  private val sessionName: String = this.getClass.getName
  private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
  protected val spark: SparkSession = SparkSessionUtils.getSession(sessionName)
  protected val unity: UnityCatalog = new UnityCatalog(spark)
  protected val batchExecutionLogger = new BatchExecutionLogger(spark, unity)

  protected val transformer: Transform
  protected val batchJobId: String
  protected val serviceName: String
  protected val sources: List[String]

  protected val service = new SparkBatchService(serviceName, spark)

  private def execute(data: DataFrame, executables: List[Executable], key: String): Unit = {

    executables.foreach { executable =>
      batchExecutionLogger.log("STARTED", batchJobId, executable.source, executable.currentOffset)
    }

    val hasMoved = executables.exists(ex => ex.currentOffset.hasMoved(ex.previousOffset))

    Try {
      val sinkName = getSink(serviceName).head
      if (hasMoved) {
        val sink = DataSink(sinkName = sinkName, data, controlTableUpdateOptions = Map(ProductMaxIdType -> key))
        service.run(Some(sink))
      }
    } match {
      case Success(_) =>
        executables.foreach { executable =>
          val status = if (executable.currentOffset.hasMoved(executable.previousOffset)) "SUCCESS" else "SKIPPED"
          batchExecutionLogger.log(status, batchJobId, executable.source, executable.currentOffset)
        }
      case Failure(exception) =>
        logger.error("Error processing batch job", exception)
        executables.foreach { executable =>
          batchExecutionLogger.log("FAILED", batchJobId, executable.source, executable.currentOffset)
          throw exception
        }
    }
  }

  def main(args: Array[String]): Unit = {
    val startId: Long = getStartId

    val executables = sources.map { source =>
      val previousOffset = batchExecutionLogger.getPreviousOffset(batchJobId, source)
      val sourceDF: Dataset[Row] = unity.read(getBronzeTableNameFor(source)).where(col("_load_time") > previousOffset.end)

      val currentOffset = batchExecutionLogger.getOffsets(sourceDF).getOrElse(previousOffset)

      Executable(source, sourceDF, previousOffset, currentOffset)
    }
    val data = executables.map { ex =>
      ex.source -> ex.df
    }.toMap

    val transformedDF =
      if (executables.length > 1) transformer.transform(data, startId) else transformer.transform(executables.head.df, startId)

    execute(transformedDF, executables, batchJobId)
  }

  private def getStartId = {
    service.prehook
    val startId = service.getStartIdUpdateStatus(batchJobId)
    startId
  }
}

case class Executable(source: String, df: DataFrame, previousOffset: Offset, currentOffset: Offset)
