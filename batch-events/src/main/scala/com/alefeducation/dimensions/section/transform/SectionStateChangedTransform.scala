package com.alefeducation.dimensions.section.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.section.transform.SectionStateChangedTransform.{sectionDeletedTransformedSink, sectionDisabledTransformedSink, sectionEnabledTransformedSink, sectionStateChangedSource}
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.{AdminSectionDeleted, AdminSectionDisabled, AdminSectionEnabled}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window

class SectionStateChangedTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(): List[Option[Sink]] = {
    import com.alefeducation.util.BatchTransformerUtility._

    val sectionStateChangedDF: Option[DataFrame] = service.readOptional(sectionStateChangedSource, session, extraProps = List(("mergeSchema", "true")))

    //To get the latest disable/enable state for each section
    val windowSpec = Window.partitionBy("uuid").orderBy(col("occurredOn").desc)

    val sectionEnableDisabledDF: Option[DataFrame] = sectionStateChangedDF.map(
      _.filter(col("EventType") === AdminSectionEnabled || col("EventType") === AdminSectionDisabled)
        .withColumn("row_number", row_number().over(windowSpec))
        .filter(col("row_number") === 1).drop("row_number")
    )

    val sectionEnabledDF: Option[DataFrame] = sectionEnableDisabledDF.map(
      _.filter(col("EventType") === AdminSectionEnabled)
        .withColumn("section_enabled", lit(true))
        .transformForUpdateDim(SectionStateChangedCols, SectionEntity, List("uuid"))
    )

    val sectionDisabledDF: Option[DataFrame] = sectionEnableDisabledDF.map(
      _.filter(col("EventType") === AdminSectionDisabled)
        .withColumn("section_enabled", lit(false))
        .transformForUpdateDim(SectionStateChangedCols, SectionEntity, List("uuid"))
    )

    val sectionDeletedDF: Option[DataFrame] = sectionStateChangedDF.map(
      _.filter(col("EventType") === AdminSectionDeleted)
        .transformForDelete(SectionDeletedCols, SectionEntity, List("uuid"))
    )

    List(
      sectionEnabledDF.map(DataSink(sectionEnabledTransformedSink, _)),
      sectionDisabledDF.map(DataSink(sectionDisabledTransformedSink, _)),
      sectionDeletedDF.map(DataSink(sectionDeletedTransformedSink, _))
    )

  }
}



object SectionStateChangedTransform {

  private val sectionStateChangedTransformService = "transform-section-state-changed-service"
  val sectionStateChangedSource: String = getSource(sectionStateChangedTransformService).head
  val sectionEnabledTransformedSink: String = getSink(sectionStateChangedTransformService).head
  val sectionDisabledTransformedSink: String = getSink(sectionStateChangedTransformService)(1)
  val sectionDeletedTransformedSink: String = getSink(sectionStateChangedTransformService)(2)

  val session: SparkSession = SparkSessionUtils.getSession(sectionStateChangedTransformService)
  val service = new SparkBatchService(sectionStateChangedTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new SectionStateChangedTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }

}
