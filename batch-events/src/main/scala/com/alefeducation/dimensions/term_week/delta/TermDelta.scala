package com.alefeducation.dimensions.term_week.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils

object TermDelta {
  val termDeltaService = "delta-term"

  val session = SparkSessionUtils.getSession(termDeltaService)
  val service = new SparkBatchService(termDeltaService, session)

  val termColumns: Seq[String] = Seq(
    "term_id",
    "term_academic_period_order",
    "term_curriculum_id",
    "term_content_academic_year_id",
    "term_start_date",
    "term_end_date",
    "term_content_repository_id",
    "term_status",
    "term_created_time",
    "term_dw_created_time",
    "term_updated_time",
    "term_deleted_time",
    "term_dw_updated_time"
  )

  val columnsToInsert: Map[String, String] = termColumns.map { column =>
    column -> s"${Alias.Events}.$column"
  }.toMap

  val columnsToUpdate: Map[String, String] = columnsToInsert -
    "term_dw_created_time" +
    ("term_dw_updated_time" -> s"${Alias.Events}.term_dw_created_time")

  def main(args: Array[String]): Unit = {

    val termTransformed = service.readOptional(getSource(termDeltaService).head, session)

    val deltaSink = termTransformed.flatMap(
      _.toUpsert(
        partitionBy = Nil,
        matchConditions = s"${Alias.Delta}.term_id = ${Alias.Events}.term_id",
        columnsToUpdate = columnsToUpdate,
        columnsToInsert = columnsToInsert
      ).map(_.toSink(getSink(termDeltaService).head))
    )

    service.run(deltaSink)
  }
}