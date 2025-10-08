package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch.{BatchWriter, Write}
import com.alefeducation.util.Resources
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DeltaWrite extends DeltaSink with BatchWriter with Write {

  def writerOptions: Map[String, String]

  def write(resource: Resources.Resource): Unit = save(name, resource, writerOptions)

}

class DeltaWriteSink(val spark: SparkSession,
                     val name: String,
                     val df: DataFrame,
                     val input: DataFrame,
                     val writerOptions: Map[String, String])
    extends DeltaWrite {}

class DeltaWriteBuilder private[delta] (
    override protected val deltaDataFrame: DataFrame,
    protected val deltaWriterOptions: Map[String, String] = Map())(implicit val sparkSession: SparkSession)
    extends DeltaSinkBuilder {

  def withWriterOptions(writerOptions: Map[String, String]): DeltaWriteBuilder = copy(writerOptions = writerOptions)

  private def copy(df: DataFrame = deltaDataFrame, writerOptions: Map[String, String] = deltaWriterOptions) =
    new DeltaWriteBuilder(df, writerOptions)

  override private[delta] def build(deltaSinkName: String, deltaEntityName: Option[String], isFact: Boolean): DeltaWriteSink =
    new DeltaWriteSink(sparkSession, deltaSinkName, deltaDataFrame, deltaDataFrame, deltaWriterOptions)

}
