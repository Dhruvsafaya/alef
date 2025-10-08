package com.alefeducation.dimensions.grade.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.dimensions.grade.transform.GradeCreatedTransform.GradeCreatedSinkName
import com.alefeducation.util.Helpers.GradeEntity
import com.alefeducation.util.Resources.getSink
import com.alefeducation.util.SparkSessionUtils

object GradeCreatedDelta {


  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  private val gradeCreatedDeltaService = "delta-grade-service"

  private val session = SparkSessionUtils.getSession(gradeCreatedDeltaService)
  val service = new SparkBatchService(gradeCreatedDeltaService, session)

  val GradeDeltaMatchCondition =
    s"""
       |${Alias.Delta}.${GradeEntity}_id = ${Alias.Events}.${GradeEntity}_id
     """.stripMargin

  val columns: Seq[String] = Seq(
    "grade_name",
    "school_id",
    "grade_id",
    "academic_year_id",
    "grade_status",
    "tenant_id",
    "grade_k12grade",
    "grade_created_time",
    "grade_dw_created_time",
    "grade_updated_time",
    "grade_deleted_time",
    "grade_dw_updated_time"
  )

  val GradeColumnsToInsert: Map[String, String] = columns.map { column =>
    column -> s"${Alias.Events}.$column"
  }.toMap

  val columnsToUpdate: Map[String, String] = GradeColumnsToInsert

  def main(args: Array[String]): Unit = {

    val gradeCreatedTransformedDF = service.readOptional(GradeCreatedSinkName, session)

    val deltaSink = gradeCreatedTransformedDF.flatMap(
      _.toUpsert(
        partitionBy = Nil,
        matchConditions = GradeDeltaMatchCondition,
        columnsToUpdate = GradeColumnsToInsert,
        columnsToInsert = columnsToUpdate
      ).map(_.toSink(getSink(gradeCreatedDeltaService).head))
    )

    service.run(deltaSink)
  }
}
