package com.alefeducation.base

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{DeltaSink => NewDeltaSink}
import com.alefeducation.bigdata.batch.{BatchSink, DeltaSink, DataSink => DataSinkAdapter}
import com.alefeducation.exception.{MissedEntityException, MissedOffsetColumnException, SinkNotConfiguredException}
import com.alefeducation.schema.internal.ControlTableUtils.{CSStatus, DataOffsetType, Failed, Processing, ProductMaxIdType}
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

trait SparkBatchServiceWriter extends WriterService {
  val name: String
  val session: SparkSession
  val dateOffsetColumnPrefix: Option[String]

  def getSinkResource(sink: Sink): Option[Resources.Resource] =
    Resources.getResourceByName(sink.name).toOption

  private def updateOptions(options: Map[String, String]): Map[String, String] = {
    val (key, url) = secretManager.getRedshiftUrlProp
    options.updated(key, url)
  }

  def save(sink: Sink): Int = {
    val resource = getSinkResource(sink).getOrElse(throw SinkNotConfiguredException(s"Sink not configured for $name : ${sink.name}"))
    val sinkFinal = sink match {
      case dataSink : DataSink => DataSinkAdapter(session, dataSink.sinkName, dataSink.dataFrame, writerOptions = updateOptions(dataSink.options))
      case dataSinkAdapter : DataSinkAdapter => dataSinkAdapter
      case deltaSink : DeltaSink => deltaSink
      case newDeltaSink : NewDeltaSink => newDeltaSink
    }
    val output = Try {
      updateMaxDwIdStatusIfRequired(sink, Processing)

      sinkFinal.write(resource)

      updateMaxDwIdIfRequired(sink, sinkFinal)
      updateDataOffsetIfRequired(sink)

    } match {
      case Success(_)  => log.info("Batch write completed successfully");1
      case Failure(ex) => {
        log.error(s"Batch not processed for due to ${ex.getMessage}", ex)
        updateMaxDwIdStatusIfRequired(sink, Failed)
        throw ex
      }
    }
    output
  }

  private def updateMaxDwIdStatusIfRequired(sink: Sink, status: CSStatus): Unit = {
    if (featureService.exists(_.isProductMaxIdConsistencyCheckEnabled)) {
      sink.controlTableForUpdate()
        .get(ProductMaxIdType)
        .foreach(controlTableService.updateStatus(_, status))
    }
  }

  private def updateMaxDwIdIfRequired(sink: Sink, sinkFinal: BatchSink): Unit = {
    for {
      key <- sink.controlTableForUpdate().get(ProductMaxIdType)
      outputDf = sinkFinal.output
      fstCol <- outputDf.columns.headOption
    } yield controlTableService.complete(key, outputDf.select(fstCol).count())
  }

  private def updateDataOffsetIfRequired(sink: Sink): Unit = {
    sink.controlTableForUpdate().get(DataOffsetType).foreach { tableName =>
      val prefix = dateOffsetColumnPrefix.getOrElse(
        throw MissedEntityException("No entity provided. It should be provided to use data offset table functionality")
      )
      val offsetColumnName: String = s"${prefix}_dw_created_time"
      if(!sink.output.columns.toSet.contains(offsetColumnName)) {
        throw MissedOffsetColumnException(s"Offset column $offsetColumnName is not found in the output dataframe")
      }

      dataOffsetService.complete(tableName, sink.output, offsetColumnName)
    }
  }
}
