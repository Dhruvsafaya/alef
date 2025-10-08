package com.alefeducation.facts.pathway_placement

import com.alefeducation.base.SparkBatchService
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType}

class PathwayPlacementTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._
  import com.alefeducation.facts.pathway_placement.PathwayPlacementTransform._

  private def transformPlacementCompleted(placementCompleted: Option[DataFrame]): Option[DataFrame] = {

    // Made to be compatible with the previous contract version. We can remove this once old data processed
    val placementWithInitialFlag = placementCompleted.map { placement =>
      if (!placement.columns.contains("isInitial")) {
        placement.withColumn("isInitial", lit(null).cast(BooleanType))
      } else {
        placement
      }
    }

    placementWithInitialFlag.map(
      _.select(col("*"), explode(col("domainGrades")).as("dg"))
        .withColumn("new_domain_name", col("dg.domainName"))
        .withColumn("new_grade_name", col("dg.grade"))
        .withColumn("placement_type", placementTypes(col("recommendationType")))
        .withColumnRenamed("placedBy", "createdBy")
        .withColumnRenamed("learnerId", "studentId")
        .withColumnRenamed("gradeLevel", "overallGrade")
        .transformForInsertFact(StudentGradeChangeMap, PathwayPlacementEntity)
    )
  }

  def transform(): Option[DataSink] = {
      val placementCompleted: Option[DataFrame] = service.readOptional(ParquetPlacementCompletionSource, session)

      val newPlacements: Option[DataFrame] = transformPlacementCompleted(placementCompleted)

      newPlacements.map(DataSink(StudentPathwayPlacementSink, _))
  }
}

object PathwayPlacementTransform {

  val PathwayPlacementTransformService = "transform-pathway-placement"

  val ParquetPlacementCompletionSource = "parquet-learning-placement-completed-source"
  val StudentPathwayPlacementSink = "transformed-student-pathway-placement-sink"
  val PathwayPlacementEntity  = "fpp"

  private val placementTypes: Column = typedLit(
    Map(
      "GRADE_CHANGE" -> 1,
      "MANUAL_PLACEMENT" -> 2,
      "LEVEL_COMPLETION" -> 3,
      "PLACEMENT_COMPLETION" -> 4,
      "BY_GRADE" -> 5,
      "AT_COURSE_BEGINNING" -> 6,
    )
  )

  val session = SparkSessionUtils.getSession(PathwayPlacementTransformService)
  val service = new SparkBatchService(PathwayPlacementTransformService, session)

  val StudentGradeChangeMap = Map(
    "placement_type" -> "fpp_placement_type",
    "new_domain_name" -> "fpp_new_pathway_domain",
    "new_grade_name" -> "fpp_new_pathway_grade",
    "studentId" -> "fpp_student_id",
    "classId" -> "fpp_class_id",
    "pathwayId" -> "fpp_pathway_id",
    "createdBy" -> "fpp_created_by",
    "overallGrade"-> "fpp_overall_grade",
    "isInitial" -> "fpp_is_initial",
    "hasAcceleratedDomains" -> "fpp_has_accelerated_domains",
    "tenantId" -> "fpp_tenant_id",
    "occurredOn" -> "occurredOn"
  )

  def main(args: Array[String]): Unit = {
    val transform = new PathwayPlacementTransform(session, service)
    service.run(transform.transform())
  }
}
