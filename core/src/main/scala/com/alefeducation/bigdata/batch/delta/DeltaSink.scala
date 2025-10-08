package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch.BatchSink
import com.alefeducation.util.Resources
import io.delta.tables.DeltaTable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DeltaException extends Exception

case class DeltaIllegalArgumentException(message: String) extends IllegalArgumentException(message) with DeltaException

case object DeltaMethodNotAllowed extends DeltaException

trait DeltaSink extends BatchSink with Logging {

  /**
    * Beware:
    * So as per current schemes, this method shall never create the deltaTable for you as it is
    * ignorant of the partitioning spec of your table.
    * Generally we rely on create events to create the delta table because they follow direct write path.
    * @param resource
    * @return
    */
  @deprecated
  protected def buildDeltaTable(resource: Resources.Resource): DeltaTable = {
    if (!DeltaTable.isDeltaTable(spark, resource.props("path"))) {
      log.info("Attempting to create empty table")
      df.limit(0)
        .write
        .format("delta")
        //.partitionBy(Columns.EventDate)
        .save(resource.props("path"))
    }
    DeltaTable.forPath(spark, resource.props("path"))
  }
}

trait DeltaMergeSink extends DeltaSink {

  def matchConditions: String

  def updateFields: Map[String, String]

}

trait DeltaSinkBuilder {

  protected def deltaDataFrame: DataFrame

  def isEmpty: Boolean = deltaDataFrame.isEmpty

  private[delta] def build(sinkName: String, entityName: Option[String], isFact: Boolean): DeltaSink

}

trait DeltaDefaultSinkBuilder extends DeltaSinkBuilder {

  implicit def sparkSession: SparkSession

  protected def deltaMatchConditions: String

  protected def deltaUpdateFields: List[String]

  protected def deltaWithoutColumns: List[String]

  protected def deltaUniqueIdColumns: List[String]

  protected def deltaMappingsOverride: Map[String, String]

  def withMatchConditions(matchConditions: String): DeltaDefaultSinkBuilder = copy(matchConditions = matchConditions)

  def withUpdateFields(updateFields: String*): DeltaDefaultSinkBuilder = copy(updateFields = updateFields.toList)

  def withoutColumns(withoutColumns: String*): DeltaDefaultSinkBuilder = copy(withoutColumns = withoutColumns.toList)

  def withUniqueIdColumns(uniqueIdColumns: String*): DeltaDefaultSinkBuilder = copy(uniqueIdColumns = uniqueIdColumns.toList)

  def withMappingsOverride(mappings: Map[String, String]): DeltaDefaultSinkBuilder = copy(mappingsOverride = mappings)

  protected def copy(df: DataFrame = deltaDataFrame,
                     matchConditions: String = deltaMatchConditions,
                     updateFields: List[String] = deltaUpdateFields,
                     withoutColumns: List[String] = deltaWithoutColumns,
                     uniqueIdColumns: List[String] = deltaUniqueIdColumns,
                     mappingsOverride: Map[String, String] = deltaMappingsOverride): DeltaDefaultSinkBuilder

  protected def getEntityName(entityName: Option[String]): String =
    entityName.getOrElse(throw DeltaIllegalArgumentException("entity name required"))

  protected def applyCommonTransformations(entity: String, isFact: Boolean): DataFrame = {
    val transform = Transform(entity = entity,
                              isFact = isFact,
                              withoutColumns = deltaWithoutColumns,
                              mappingsOverride = deltaMappingsOverride,
                              uniqueIdColumns = deltaUniqueIdColumns)
    DeltaUpdate.applyTransformations(deltaDataFrame, transform)
  }

}
