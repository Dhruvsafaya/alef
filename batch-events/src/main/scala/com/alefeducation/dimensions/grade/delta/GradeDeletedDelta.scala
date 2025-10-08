package com.alefeducation.dimensions.grade.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.dimensions.grade.delta.GradeCreatedDelta.{GradeColumnsToInsert, GradeDeltaMatchCondition}
import com.alefeducation.dimensions.grade.transform.GradeDeletedTransform.GradeDeletedSinkName
import com.alefeducation.util.Resources.getSink
import com.alefeducation.util.SparkSessionUtils

object GradeDeletedDelta {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  private val gradeDeletedService = "delta-grade-service"

  private val session = SparkSessionUtils.getSession(gradeDeletedService)
  val service = new SparkBatchService(gradeDeletedService, session)

  private val GradeColumnsDeletedEvent: Map[String, String] = Map(
    "grade_id" -> s"${Alias.Events}.grade_id",
    "grade_dw_updated_time" -> s"${Alias.Events}.grade_dw_created_time",
    "grade_deleted_time" -> s"${Alias.Events}.grade_created_time",
    "grade_status" -> s"${Alias.Events}.grade_status"
  )

  def main(args: Array[String]): Unit = {

    val gradeDeletedTransformedDF = service.readOptional(GradeDeletedSinkName, session)

    val deltaSink = gradeDeletedTransformedDF.flatMap(
      _.toUpsert(
        partitionBy = Nil,
        matchConditions = GradeDeltaMatchCondition,
        columnsToInsert = GradeColumnsDeletedEvent,
        columnsToUpdate = GradeColumnsDeletedEvent
      ).map(_.toSink(getSink(gradeDeletedService).head))
    )

    service.run(deltaSink)
  }
}
