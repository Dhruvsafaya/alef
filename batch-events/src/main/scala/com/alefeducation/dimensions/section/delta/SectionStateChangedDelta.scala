package com.alefeducation.dimensions.section.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.dimensions.section.delta.SectionMutatedDelta.sectionDeltaMatchCondition
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils

object SectionStateChangedDelta {


  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  private val sectionStateChangedDeltaService = "delta-section-service"

  private val session = SparkSessionUtils.getSession(sectionStateChangedDeltaService)
  val service = new SparkBatchService(sectionStateChangedDeltaService, session)

  private val sectionStateChangedDeltaCols: Seq[String] = Seq(
    "section_status",
    "section_enabled",
    "section_updated_time",
    "section_deleted_time",
    "section_dw_updated_time"
  )

  val sectionColumnsToInsert: Map[String, String] = sectionStateChangedDeltaCols.map { column =>
    column -> s"${Alias.Events}.$column"
  }.toMap

  val sectionColumnsToUpdateEnabledDisabled: Map[String, String] = sectionColumnsToInsert -
    ("section_deleted_time", "section_updated_time") +
    ("section_dw_updated_time" -> s"${Alias.Events}.section_dw_created_time")

  val sectionColumnsToUpdateDeleted: Map[String, String] = sectionColumnsToInsert -
    ("section_enabled", "section_updated_time") +
    ("section_dw_updated_time" -> s"${Alias.Events}.section_dw_created_time")

  private def processSectionStateChangedDelta(sourceIndex: Int, columnsToInsert: Map[String, String], columnsToUpdate: Map[String, String]): Unit = {
    val transformedDF = service.readOptional(getSource(sectionStateChangedDeltaService)(sourceIndex), session)

    val deltaSink = transformedDF.flatMap(
      _.toUpsert(
        partitionBy = Nil,
        matchConditions = sectionDeltaMatchCondition,
        columnsToInsert = columnsToInsert,
        columnsToUpdate = columnsToUpdate
      ).map(_.toSink(getSink(sectionStateChangedDeltaService).head))
    )

    service.run(deltaSink)
  }

  def main(args: Array[String]): Unit = {

    processSectionStateChangedDelta(2, sectionColumnsToUpdateEnabledDisabled, sectionColumnsToUpdateEnabledDisabled) // Process sectionEnabled
    processSectionStateChangedDelta(3, sectionColumnsToUpdateEnabledDisabled, sectionColumnsToUpdateEnabledDisabled) // Process sectionDisabled
    processSectionStateChangedDelta(4, sectionColumnsToUpdateDeleted, sectionColumnsToUpdateDeleted) // Process sectionDeleted

  }
}
