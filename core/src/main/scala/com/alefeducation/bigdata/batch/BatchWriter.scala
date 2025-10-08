package com.alefeducation.bigdata.batch

import com.alefeducation.util.Constants.{Format, Mode, PartitionBy, Path}
import com.alefeducation.util.Resources
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}

import scala.annotation.tailrec

trait BatchWriter extends Logging {

  def df: DataFrame

  def spark: SparkSession

  private def outputFolderName(value: String): String = {
    val res = value.replaceAll("/processing/", s"/data/")
    res
  }

  @tailrec
  private def setWriterOptions(itr: Iterator[(String, String)], writer: DataFrameWriter[Row]): DataFrameWriter[Row] =
    if (itr.isEmpty) writer
    else {
      val (key, value) = itr.next()
      key match {
        case Format                      => setWriterOptions(itr, writer.format(value))
        case Mode                        => setWriterOptions(itr, writer.mode(value))
        case Path if value contains "s3" => setWriterOptions(itr, writer.option(key, outputFolderName(value)))
        case Path                        => setWriterOptions(itr, writer.option(key, value + spark.conf.get("_batchDate")))
        case PartitionBy                 => setWriterOptions(itr, writer.partitionBy(value.split(",").toList: _*))
        case _                           => setWriterOptions(itr, writer.option(key, value))
      }
    }

  def save(sinkName: String, resource: Resources.Resource, writerOptions: BatchWriter.WriterOptions): Unit = {
    val itr = (resource.props ++ writerOptions).iterator
    log.info("Writing batch results...")
    val coalesceValue = Resources.getCoalesceValue()
    val output =
      if ((sinkName contains "xapi") || (sinkName contains "behavioral"))
        df.coalesce(coalesceValue)
      else
        df.distinct.coalesce(coalesceValue)
    val writer = setWriterOptions(itr, output.write)
    writer.save()
  }

}

object BatchWriter {

  type WriterOptions = Map[String, String]

  object Options {

    val PartitionBy = "partitionBy"
    val Path = "path"

  }

}
