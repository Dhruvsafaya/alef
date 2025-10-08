package com.alefeducation.dimensions.section.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils

object SectionStateChangedRedshift {

  private val sectionStateChangedRedshiftService = "redshift-section-service"
  private val sectionTargetTableName = "dim_section"

  private val session = SparkSessionUtils.getSession(sectionStateChangedRedshiftService)
  val service = new SparkBatchService(sectionStateChangedRedshiftService, session)

  private val sectionDimensionCols: Seq[String] = Seq(
    "section_status",
    "section_enabled",
    "section_updated_time",
    "section_deleted_time",
    "section_dw_updated_time"
  )

  val sectionColumnsToInsert: Map[String, String] = sectionDimensionCols.map { column =>
    column -> s"$TempTableAlias.$column"
  }.toMap

  private val sectionColumnsToUpdateEnableDisable: Map[String, String] = sectionColumnsToInsert -
    ("section_deleted_time", "section_updated_time") +
    ("section_dw_updated_time" -> s"$TempTableAlias.section_dw_created_time")

  private val sectionColumnsToUpdateDeleted: Map[String, String] =
    sectionColumnsToInsert -
    ("section_enabled", "section_updated_time") +
    ("section_dw_updated_time" -> s"$TempTableAlias.section_dw_created_time")

  def main(args: Array[String]): Unit = {

    service.run {
      val sectionEnabledTransformedDF = service.readOptional(getSource(sectionStateChangedRedshiftService)(2), session)

      sectionEnabledTransformedDF.map(
        _.toRedshiftUpsertSink(
          sinkName = getSink(sectionStateChangedRedshiftService).head,
          tableName = sectionTargetTableName,
          matchConditions = s"$sectionTargetTableName.section_id = $TempTableAlias.section_id",
          columnsToInsert = sectionColumnsToUpdateEnableDisable,
          columnsToUpdate = sectionColumnsToUpdateEnableDisable,
          isStaging = false
        )
      )
    }

    service.run {
      val sectionDisabledTransformedDF = service.readOptional(getSource(sectionStateChangedRedshiftService)(3), session)

      sectionDisabledTransformedDF.map(
        _.toRedshiftUpsertSink(
          sinkName = getSink(sectionStateChangedRedshiftService).head,
          tableName = sectionTargetTableName,
          matchConditions = s"$sectionTargetTableName.section_id = $TempTableAlias.section_id",
          columnsToInsert = sectionColumnsToUpdateEnableDisable,
          columnsToUpdate = sectionColumnsToUpdateEnableDisable,
          isStaging = false
        )
      )
    }

    service.run {
      val sectionDeletedTransformedDF = service.readOptional(getSource(sectionStateChangedRedshiftService)(4), session)

      sectionDeletedTransformedDF.map(
        _.toRedshiftUpsertSink(
          sinkName = getSink(sectionStateChangedRedshiftService).head,
          tableName = sectionTargetTableName,
          matchConditions = s"$sectionTargetTableName.section_id = $TempTableAlias.section_id",
          columnsToInsert = sectionColumnsToUpdateDeleted,
          columnsToUpdate = sectionColumnsToUpdateDeleted,
          isStaging = false
        )
      )
    }
  }
}