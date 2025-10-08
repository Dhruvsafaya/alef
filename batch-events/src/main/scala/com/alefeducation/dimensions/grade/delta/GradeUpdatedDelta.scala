package com.alefeducation.dimensions.grade.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.dimensions.grade.delta.GradeCreatedDelta.{GradeColumnsToInsert, GradeDeltaMatchCondition}
import com.alefeducation.dimensions.grade.transform.GradeUpdatedTransform.GradeUpdatedSinkName
import com.alefeducation.util.Resources.getSink
import com.alefeducation.util.SparkSessionUtils

object GradeUpdatedDelta {

  val GradeColumnsToUpdate: Map[String, String] = GradeColumnsToInsert -
    ("grade_created_time", "grade_dw_created_time", "grade_updated_time", "grade_deleted_time", "grade_dw_updated_time") +
    (
      "grade_updated_time" -> s"${Alias.Events}.grade_created_time",
      "grade_dw_updated_time" -> s"${Alias.Events}.grade_dw_created_time"
    )

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  private val gradeUpdatedService = "delta-grade-service"

  private val session = SparkSessionUtils.getSession(gradeUpdatedService)
  val service = new SparkBatchService(gradeUpdatedService, session)

  def main(args: Array[String]): Unit = {

    val gradeUpdatedTransformedDF = service.readOptional(GradeUpdatedSinkName, session)

    val deltaSink = gradeUpdatedTransformedDF.flatMap(
      _.toUpsert(
        partitionBy = Nil,
        matchConditions = GradeDeltaMatchCondition,
        columnsToInsert= GradeColumnsToInsert,
        columnsToUpdate = GradeColumnsToUpdate
      ).map(_.toSink(getSink(gradeUpdatedService).head))
    )

    service.run(deltaSink)
  }
}
