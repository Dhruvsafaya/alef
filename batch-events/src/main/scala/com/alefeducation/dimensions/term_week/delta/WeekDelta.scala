package com.alefeducation.dimensions.term_week.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils

object WeekDelta {
  val weekDeltaService = "delta-week"

  val session = SparkSessionUtils.getSession(weekDeltaService)
  val service = new SparkBatchService(weekDeltaService, session)

  val weekColumns: Seq[String] = Seq(
    "week_id",
    "week_term_id",
    "week_number",
    "week_start_date",
    "week_end_date",
    "week_status",
    "week_created_time",
    "week_dw_created_time",
    "week_updated_time",
    "week_deleted_time",
    "week_dw_updated_time"
  )

  val columnsToInsert: Map[String, String] = weekColumns.map { column =>
    column -> s"${Alias.Events}.$column"
  }.toMap

  val columnsToUpdate: Map[String, String] = columnsToInsert -
    "week_dw_created_time" +
    ("week_dw_updated_time" -> s"${Alias.Events}.week_dw_created_time")

  def main(args: Array[String]): Unit = {

    val weekTransformed = service.readOptional(getSource(weekDeltaService).head, session)

    val deltaSink = weekTransformed.flatMap(
      _.toUpsert(
        partitionBy = Nil,
        matchConditions = s"${Alias.Delta}.week_id = ${Alias.Events}.week_id",
        columnsToUpdate = columnsToUpdate,
        columnsToInsert = columnsToInsert
      ).map(_.toSink(getSink(weekDeltaService).head))
    )

    service.run(deltaSink)
  }
}