package com.alefeducation.service

import com.alefeducation.bigdata.Sink
import com.alefeducation.exception.{NoParquetFileException, SinkNotConfiguredException}
import com.alefeducation.util.AwsUtils.isConnectionEnabled
import com.alefeducation.util.Constants.{Format, OutputMode, PartitionBy, Path}
import com.alefeducation.util.Resources.{getString, loadMap}
import com.alefeducation.util.SparkSessionUtils.getKafkaDataFrame
import com.alefeducation.util.{Constants, Resources}
import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

trait SparkStreamingService extends Service {

  override def prehook: Unit = {
    secretManager.loadCertificates()
  }

  override def read(config: String,
                    session: SparkSession,
                    isMandatory: Boolean = true,
                    optionalSchema: Option[StructType] = None): DataFrame =
    Try {
      val props: List[(String, String)] = loadMap(config).toList
      if (isConnectionEnabled(session)) {
        val kafkaProps = secretManager.getKafkaProps
        setSourceOps(props ++ kafkaProps, session.readStream).load
      } else {
        getKafkaDataFrame(session, config)
      }
    } match {
      case Success(df) => df
      case Failure(e) =>
        if (isMandatory) throw new NoParquetFileException(s"Could not find dataframe $config due to ${e.getMessage}")
        else session.emptyDataFrame
    }

  override def write(sinks: => List[Sink]): Unit = {
    import session.implicits._
    log.info("Writing data transformations...")

    val streamingStartDate = getString("streaming-start-date")
    sinks
      .flatMap {
        case dataSink: DataSink =>
          val (propName, result, options) = (dataSink.sinkName, dataSink.dataFrame, dataSink.options)
          val dataFrame = if (streamingStartDate.nonEmpty) result.filter($"occurredOn" >= streamingStartDate) else result
          val sinks = Resources.getResource(name, "sink")
          val sink = sinks.find(_.name == propName)
          if (sink.isEmpty) throw SinkNotConfiguredException(s"Sink not configured for $name")

          sink.map { resource =>
            val props: List[(String, String)] = (resource.props ++ options).toList
            log.info("Writing streaming results...")

            val dataStream = createDataStream(props, dataFrame)
            List(setStreamSinkOps(props, dataStream).start)
          }
        case _ => None
      }
      .flatten
      .foreach(_.awaitTermination)

    secretManager.disposeCertificates()
  }

  private def createDataStream(props: List[(String, String)], dataFrame: DataFrame): DataStreamWriter[Row] = {
    dataFrame.writeStream.foreachBatch { (df: DataFrame, _: Long) =>
      if (!df.isEmpty) {
        setWriterSinkOps(props, df.write).save()
      }
    }
  }

  @tailrec
  private def setSourceOps(props: List[(String, String)], reader: DataStreamReader): DataStreamReader = props match {
    case Nil                   => reader
    case (Format, value) :: xs => setSourceOps(xs, reader.format(value))
    case (key, value) :: xs    => setSourceOps(xs, reader.option(key, value))
  }

  @tailrec
  private def setStreamSinkOps(props: List[(String, String)], writer: DataStreamWriter[Row]): DataStreamWriter[Row] = props match {
    case Nil                                                                       => writer
    case (Constants.Trigger, value) :: xs if value.trim.toLowerCase.equals("once") => setStreamSinkOps(xs, writer.trigger(Trigger.Once()))
    case (Constants.Trigger, value) :: xs                                          => setStreamSinkOps(xs, writer.trigger(Trigger.ProcessingTime(value)))
    case (Path, value) :: xs if value contains "s3"                                =>
      setStreamSinkOps(xs, writer.option(Path, value).option("checkpointLocation", value.replaceAll("/incoming/", "/checkpoint/")))
    case _ :: xs => setStreamSinkOps(xs, writer)
  }

  @tailrec
  private def setWriterSinkOps(props: List[(String, String)], writer: DataFrameWriter[Row]): DataFrameWriter[Row] = props match {
    case Nil                                                                       => writer
    case (Format, value) :: xs                                                     => setWriterSinkOps(xs, writer.format(value))
    case (OutputMode, value) :: xs                                                 => setWriterSinkOps(xs, writer.mode(value))
    case (PartitionBy, value) :: xs                                                => setWriterSinkOps(xs, writer.partitionBy(value.split(",").toList: _*))
    case (Path, value) :: xs if value contains "s3"                                => setWriterSinkOps(xs, writer.option(Path, value))
    case (key, value) :: xs => setWriterSinkOps(xs, writer.option(key, value))
  }
}
