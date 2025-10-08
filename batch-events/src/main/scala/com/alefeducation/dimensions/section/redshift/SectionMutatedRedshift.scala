package com.alefeducation.dimensions.section.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils

object SectionMutatedRedshift {

  private val sectionMutatedRedshiftService = "redshift-section-service"
  private val sectionTargetTableName = "dim_section"

  private val session = SparkSessionUtils.getSession(sectionMutatedRedshiftService)
  val service = new SparkBatchService(sectionMutatedRedshiftService, session)

  val sectionDimensionCols: Seq[String] = Seq(
    "section_id",
    "section_status",
    "section_alias",
    "section_name",
    "section_enabled",
    "section_source_id",
    "tenant_id",
    "grade_id",
    "school_id",
    "section_created_time",
    "section_updated_time",
    "section_deleted_time",
    "section_dw_created_time",
    "section_dw_updated_time"
  )

  val sectionColumnsToInsert: Map[String, String] = sectionDimensionCols.map { column =>
    column -> s"$TempTableAlias.$column"
  }.toMap

  private val sectionColumnsToUpdate: Map[String, String] = sectionColumnsToInsert -
    ("section_dw_created_time", "section_created_time") +
    ("section_dw_updated_time" -> s"$TempTableAlias.section_dw_created_time",
      "section_updated_time" -> s"$TempTableAlias.section_created_time")

  def main(args: Array[String]): Unit = {

    service.run {
      val sectionCreatedTransformedDF = service.readOptional(getSource(sectionMutatedRedshiftService).head, session)

      sectionCreatedTransformedDF.map(
        _.toRedshiftUpsertSink(
          sinkName = getSink(sectionMutatedRedshiftService).head,
          tableName = sectionTargetTableName,
          matchConditions = s"$sectionTargetTableName.section_id = $TempTableAlias.section_id",
          columnsToInsert = sectionColumnsToInsert,
          columnsToUpdate = sectionColumnsToUpdate,
          isStaging = false
        )
      )
    }

    service.run {
      val sectionUpdatedTransformedDF = service.readOptional(getSource(sectionMutatedRedshiftService)(1), session)

      sectionUpdatedTransformedDF.map(
        _.toRedshiftUpsertSink(
          sinkName = getSink(sectionMutatedRedshiftService).head,
          tableName = sectionTargetTableName,
          matchConditions = s"$sectionTargetTableName.section_id = $TempTableAlias.section_id",
          columnsToInsert = sectionColumnsToInsert,
          columnsToUpdate = sectionColumnsToUpdate,
          isStaging = false
        )
      )
    }
  }
}