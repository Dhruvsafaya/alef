package com.alefeducation.facts.pathway.leaderboard

import com.alefeducation.base.SparkBatchService
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.DateType

class PathwayLeaderboardTransform(val session: SparkSession,
                                  val service: SparkBatchService) {

  import PathwayLeaderboardTransform._
  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {
    val sourceName = getSource(PathwayLeaderboardTransformService).headOption
    val source = sourceName.flatMap(service.readOptional(_, session))

    val transformed = source.map(
      _.withColumn("startDate", col("startDate").cast(DateType))
        .withColumn("endDate", col("endDate").cast(DateType))
        .select(col("*"), explode(col("leaders")))
        .select(col("*"), col("col.*"))
        .transformForInsertFact(PathwayLeaderboardCols, PathwayLeaderboardEntity)
    )

    for {
      sinkName <- getSink(PathwayLeaderboardTransformService).headOption
      df <- transformed
    } yield DataSink(sinkName, df)
  }
}

object PathwayLeaderboardTransform {

  val PathwayLeaderboardTransformService = "transform-pathway-leaderboard"
  val PathwayLeaderboardEntity = "fpl"

  val PathwayLeaderboardCols: Map[String, String] = Map(
    "tenantId" -> s"${PathwayLeaderboardEntity}_tenant_id",
    "id" -> s"${PathwayLeaderboardEntity}_id",
    "classId" -> s"${PathwayLeaderboardEntity}_class_id",
    "pathwayId" -> s"${PathwayLeaderboardEntity}_pathway_id",
    "gradeId" -> s"${PathwayLeaderboardEntity}_grade_id",
    "academicYearId" -> s"${PathwayLeaderboardEntity}_academic_year_id",
    "startDate" -> s"${PathwayLeaderboardEntity}_start_date",
    "endDate" -> s"${PathwayLeaderboardEntity}_end_date",
    "studentId" -> s"${PathwayLeaderboardEntity}_student_id",
    "order" -> s"${PathwayLeaderboardEntity}_order",
    "progress" -> s"${PathwayLeaderboardEntity}_level_competed_count",
    "averageScore" -> s"${PathwayLeaderboardEntity}_average_score",
    "totalStars" -> s"${PathwayLeaderboardEntity}_total_stars",
    "occurredOn" -> "occurredOn"
  )

  val session: SparkSession = SparkSessionUtils.getSession(PathwayLeaderboardTransformService)
  val service = new SparkBatchService(PathwayLeaderboardTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new PathwayLeaderboardTransform(session, service)
    service.run(transformer.transform())
  }
}
