package com.alefeducation.dimensions.grade.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.grade.redshift.GradeCreatedRedshift.{GradeTargetTableName, GradeColumnsToInsert}
import com.alefeducation.dimensions.grade.transform.GradeUpdatedTransform.GradeUpdatedSinkName
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.getSink
import com.alefeducation.util.SparkSessionUtils

object GradeUpdatedRedshift {

  private val gradeUpdatedRedshiftService = "redshift-grade-service"

  private val session = SparkSessionUtils.getSession(gradeUpdatedRedshiftService)
  val service = new SparkBatchService(gradeUpdatedRedshiftService, session)

  val GradeColumnsToUpdate: Map[String, String] = GradeColumnsToInsert -
      ("grade_dw_created_time", "grade_created_time", "grade_deleted_time") +
      (
        "grade_dw_updated_time" -> s"$TempTableAlias.grade_dw_created_time",
        "grade_updated_time" -> s"$TempTableAlias.grade_created_time"
      )

  def main(args: Array[String]): Unit = {
    service.run {
      val gradeUpdatedTransformedDF = service.readOptional(GradeUpdatedSinkName, session)

      gradeUpdatedTransformedDF.map(
        _.toRedshiftUpsertSink(
          sinkName = getSink(gradeUpdatedRedshiftService).head,
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