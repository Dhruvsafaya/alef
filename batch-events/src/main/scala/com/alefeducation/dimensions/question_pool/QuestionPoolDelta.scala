package com.alefeducation.dimensions.question_pool

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

object QuestionPoolDelta {
  val QuestionPoolDeltaService = "delta-ccl-question-pool-mutated-service"
  val targetTableName = "dim_question_pool"

  val session = SparkSessionUtils.getSession(QuestionPoolDeltaService)
  val service = new SparkBatchService(QuestionPoolDeltaService, session)

  val columns: Seq[String] = Seq(
    "question_pool_id",
    "question_pool_name",
    "question_pool_app_status",
    "question_pool_triggered_by",
    "question_pool_question_code_prefix",
    "question_pool_status",
    "question_pool_created_time",
    "question_pool_dw_created_time",
    "question_pool_updated_time",
    "question_pool_deleted_time"
  )

  val columnsToInsert: Map[String, String] = columns.map { column =>
    column -> s"${Alias.Events}.$column"
  }.toMap

  val columnsToUpdate: Map[String, String] = columnsToInsert -
    "question_pool_dw_created_time" +
    ("question_pool_dw_updated_time" -> s"${Alias.Events}.question_pool_dw_updated_time")

  def main(args: Array[String]): Unit = {

    val QuestionPoolTransformed = service.readOptional(getSource(QuestionPoolDeltaService).head, session)

    val deltaSink = QuestionPoolTransformed.flatMap(
      _.toUpsert(
        partitionBy = Nil,
        matchConditions = s"${Alias.Delta}.question_pool_id = ${Alias.Events}.question_pool_id",
        columnsToUpdate = columnsToUpdate,
        columnsToInsert = columnsToInsert
      ).map(_.toSink(getSink(QuestionPoolDeltaService).head))
    )

    service.run(deltaSink)
  }
}