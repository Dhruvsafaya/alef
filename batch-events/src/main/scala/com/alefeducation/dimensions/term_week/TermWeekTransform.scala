package com.alefeducation.dimensions.term_week

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.admin.{ContentRepositoryRedshift, DwIdMapping}
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class TermWeekTransform(val session: SparkSession, val service: SparkBatchService) {

  import TermWeekTransform._
  import org.apache.spark.sql.functions._
  import session.implicits._

  def transform(): List[Option[Sink]] = {
    import com.alefeducation.util.BatchTransformerUtility._

    val termWeekDF: Option[DataFrame] = service.readOptional(TermWeekCreatedSourceName, session, extraProps = List(("mergeSchema", "true")))

    val dwIdMappingDf =  service.readFromRedshift[ContentRepositoryRedshift](
      RedshiftContentRepositorySource,
      selectedCols = List("content_repository_id", "content_repository_dw_id")
    )


    val termWithContentRepositoryDwId = termWeekDF.map(
      _.join(dwIdMappingDf, $"contentRepositoryId" === $"content_repository_id", "left")
      .drop("content_repository_id").dropDuplicates()
    )

    val termTransformedDf: Option[DataFrame] = termWithContentRepositoryDwId.map(
      _.withColumn("startDate", col("startDate").cast("date"))
      .withColumn("endDate", col("endDate").cast("date"))
        .transformForInsertOrUpdate(termColumns, termEntity))

    val weekDF: Option[DataFrame] = termWeekDF.map{ df =>

      val weekDFCols:DataFrame = df.select(
        col("id"),
        col("endDate").cast("date"),
        col("weeks"),
        col("occurredOn"),
        col("eventType")
      )

      weekDFCols
        .withColumn("weeks", explode(col("weeks")))
        .withColumn("startDate",col("weeks.startDate").cast("date"))

    }

    val weekTransformedDf: Option[DataFrame] = weekDF.map(_.transformForInsertOrUpdate(weekColumns, weekEntity))

    List(
      termTransformedDf.map(DataSink(TermCreatedSinkName, _)),
      weekTransformedDf.map(DataSink(WeekCreatedSinkName, _))
    )
  }
}

object TermWeekTransform {

  val termColumns: Map[String, String] = Map(
    "id" -> "term_id",
    "curriculumId" -> "term_curriculum_id",
    "number" -> "term_academic_period_order",
    "curriculumId" -> "term_curriculum_id",
    "academicYearId" -> "term_content_academic_year_id",
    "startDate" -> "term_start_date",
    "endDate" -> "term_end_date",
    "contentRepositoryId" -> "term_content_repository_id",
    "content_repository_dw_id" -> "term_content_repository_dw_id",
    "occurredOn" -> "occurredOn",
    "term_status" -> "term_status"
  )

  val weekColumns: Map[String, String] = Map(
    "weeks.id" -> "week_id",
    "weeks.number" -> "week_number",
    "startDate" -> "week_start_date",
    "endDate" -> "week_end_date",
    "id" -> "week_term_id",
    "occurredOn" -> "occurredOn",
    "week_status" -> "week_status"
  )

  private val TermWeekCreatedTransformService: String = "transform-term-week"

  val TermWeekCreatedSourceName: String = getSource(TermWeekCreatedTransformService).head
  val TermCreatedSinkName: String = getSink(TermWeekCreatedTransformService).head
  val WeekCreatedSinkName: String = getSink(TermWeekCreatedTransformService)(1)

  val session: SparkSession = SparkSessionUtils.getSession(TermWeekCreatedTransformService)
  val service = new SparkBatchService(TermWeekCreatedTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new TermWeekTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }

}
