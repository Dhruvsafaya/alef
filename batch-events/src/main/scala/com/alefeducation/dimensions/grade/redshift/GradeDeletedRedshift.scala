package com.alefeducation.dimensions.grade.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.grade.redshift.GradeCreatedRedshift.{GradeColumnsToInsert, GradeTargetTableName}
import com.alefeducation.dimensions.grade.transform.GradeDeletedTransform.GradeDeletedSinkName
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.getSink
import com.alefeducation.util.SparkSessionUtils

object GradeDeletedRedshift {

  private val gradeDeletedRedshiftService = "redshift-grade-service"

  private val session = SparkSessionUtils.getSession(gradeDeletedRedshiftService)
  val service = new SparkBatchService(gradeDeletedRedshiftService, session)

  val GradeColumnsDeletedEvent: Map[String, String] = Map(
    "grade_id" -> s"$TempTableAlias.grade_id",
    "grade_dw_updated_time" -> s"$TempTableAlias.grade_dw_created_time",
    "grade_deleted_time" -> s"$TempTableAlias.grade_created_time",
    "grade_status" -> s"$TempTableAlias.grade_status"
  )

  def main(args: Array[String]): Unit = {
    service.run {
      val gradeDeletedTransformedDF = service.readOptional(GradeDeletedSinkName, session)

      gradeDeletedTransformedDF.map(
        _.toRedshiftUpsertSink(
          sinkName = getSink(gradeDeletedRedshiftService).head,
          tableName = GradeTargetTableName,
          matchConditions = s"$GradeTargetTableName.grade_id = $TempTableAlias.grade_id",
          columnsToInsert = GradeColumnsDeletedEvent,
          columnsToUpdate = GradeColumnsDeletedEvent,
          isStaging = false
        )
      )
    }
  }
}