package com.alefeducation.dimensions.grade.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.grade.transform.GradeCreatedTransform.GradeCreatedSinkName
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.AdminGradeCreated
import com.alefeducation.util.Resources.getSink
import com.alefeducation.util.SparkSessionUtils

object GradeCreatedRedshift {

  private val gradeCreatedRedshiftService = "redshift-grade-service"
  val GradeTargetTableName = "dim_grade"

  private val session = SparkSessionUtils.getSession(gradeCreatedRedshiftService)
  val service = new SparkBatchService(gradeCreatedRedshiftService, session)

  val GradeDimensionCols: Seq[String] = Seq(
    "grade_id",
    "grade_name",
    "grade_k12grade",
    "academic_year_id",
    "tenant_id",
    "school_id",
    "grade_status",
    "grade_created_time",
    "grade_updated_time",
    "grade_deleted_time",
    "grade_dw_created_time",
    "grade_dw_updated_time"
  )

  val GradeColumnsToInsert: Map[String, String] = GradeDimensionCols.map { column =>
    column -> s"$TempTableAlias.$column"
  }.toMap

  val GradeColumnsToUpdate: Map[String, String] = GradeColumnsToInsert -
    ("grade_dw_created_time", "grade_deleted_time") +
    ("grade_dw_updated_time" -> s"$TempTableAlias.grade_dw_created_time")

  def main(args: Array[String]): Unit = {

    service.run {
      val gradeCreatedTransformedDF = service.readOptional(GradeCreatedSinkName, session)

      gradeCreatedTransformedDF.map(
        _.toRedshiftUpsertSink(
          sinkName = getSink(gradeCreatedRedshiftService).head,
          tableName = GradeTargetTableName,
          matchConditions = s"$GradeTargetTableName.grade_id = $TempTableAlias.grade_id",
          columnsToInsert = GradeColumnsToInsert,
          columnsToUpdate = GradeColumnsToUpdate,
          isStaging = false
        )
      )
    }
  }
}