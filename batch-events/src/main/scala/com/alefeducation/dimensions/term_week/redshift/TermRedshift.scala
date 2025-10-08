package com.alefeducation.dimensions.term_week.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils

object TermRedshift {
  private val termRedshiftService = "redshift-term"
  private val termTableName = "dim_term"

  private val session = SparkSessionUtils.getSession(termRedshiftService)
  val service = new SparkBatchService(termRedshiftService, session)

  val columns: Seq[String] = Seq(
    "term_id",
    "term_created_time",
    "term_updated_time",
    "term_deleted_time",
    "term_dw_created_time",
    "term_dw_updated_time",
    "term_academic_period_order",
    "term_curriculum_id",
    "term_content_academic_year_id",
    "term_start_date",
    "term_end_date",
    "term_status",
    "term_content_repository_id",
    "term_content_repository_dw_id"
  )

  val termColumnsToInsert: Map[String, String] = columns.map { column =>
    column -> s"$TempTableAlias.$column"
  }.toMap

  val termColumnsToUpdate: Map[String, String] = termColumnsToInsert -
    "term_dw_created_time" +
    ("term_dw_updated_time" -> s"$TempTableAlias.term_dw_created_time")

  def main(args: Array[String]): Unit = {
    service.run {
      val termCreatedTransformed = service.readOptional(getSource(termRedshiftService).head, session)

      termCreatedTransformed.map(_.toRedshiftUpsertSink(
          sinkName = getSink(termRedshiftService).head,
          tableName = termTableName,
          matchConditions = s"$termTableName.term_id = $TempTableAlias.term_id",
          columnsToInsert = termColumnsToInsert,
          columnsToUpdate = termColumnsToUpdate,
          isStaging = false
        )
      )
    }
  }
}