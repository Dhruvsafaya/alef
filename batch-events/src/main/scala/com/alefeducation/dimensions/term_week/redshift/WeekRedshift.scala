package com.alefeducation.dimensions.term_week.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils

object WeekRedshift {
  private val weekRedshiftService = "redshift-week"
  private val weekTableName = "dim_week"

  private val session = SparkSessionUtils.getSession(weekRedshiftService)
  val service = new SparkBatchService(weekRedshiftService, session)

  val columns: Seq[String] = Seq(
    "week_id",
    "week_number",
    "week_status",
    "week_start_date",
    "week_end_date",
    "week_term_id",
    "week_created_time",
    "week_updated_time",
    "week_deleted_time",
    "week_dw_created_time",
    "week_dw_updated_time"
  )

  val weekColumnsToInsert: Map[String, String] = columns.map { column =>
    column -> s"$TempTableAlias.$column"
  }.toMap

  val weekColumnsToUpdate: Map[String, String] = weekColumnsToInsert -
    "week_dw_created_time" +
    ("week_dw_updated_time" -> s"$TempTableAlias.week_dw_created_time")

  def main(args: Array[String]): Unit = {
    service.run {
      val weekCreatedTransformed = service.readOptional(getSource(weekRedshiftService).head, session)

      weekCreatedTransformed.map(_.toRedshiftUpsertSink(
          sinkName = getSink(weekRedshiftService).head,
          tableName = weekTableName,
          matchConditions = s"$weekTableName.week_id = $TempTableAlias.week_id",
          columnsToInsert = weekColumnsToInsert,
          columnsToUpdate = weekColumnsToUpdate,
          isStaging = false
        )
      )
    }
  }
}