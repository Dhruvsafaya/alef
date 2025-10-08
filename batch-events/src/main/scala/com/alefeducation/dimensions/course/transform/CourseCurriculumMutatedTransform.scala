package com.alefeducation.dimensions.course.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.course.transform.CourseCurriculumHelper.{extractCurriculums, getCombinedCurriculums}
import com.alefeducation.dimensions.course.transform.CourseCurriculumMutatedTransform._
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.CourseInactiveStatusVal
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, explode, lit, size}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

class CourseCurriculumMutatedTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(): List[Sink] = {
    val coursePublished: Option[DataFrame] =
      service.readOptional(CoursePublishedParquetSource, session, extraProps = List(("mergeSchema", "true")))

    val courseCurriculumsPublished: Option[DataFrame] = extractCurriculums(coursePublished)

    val courseDetailsUpdated: Option[DataFrame] =
      service.readOptional(CourseDetailsUpdatedParquetSource, session, extraProps = List(("mergeSchema", "true")))

    val finalDF: Option[DataFrame] = getCombinedCurriculums(session, courseCurriculumsPublished, courseDetailsUpdated)

    val startId = service.getStartIdUpdateStatus(CourseCurriculumKey)
    val curriculumMutated = finalDF.flatMap(
      _.transformForIWH2(
        CourseCurriculumCols,
        CourseCurriculumEntityPrefix,
        0,
        attachedEvents = Nil,
        detachedEvents = Nil,
        List("id"),
        inactiveStatus = CourseInactiveStatusVal
      ).genDwId("cc_dw_id", startId).checkEmptyDf
    )

    curriculumMutated
      .map(DataSink(CourseCurriculumMutatedTransformedSink, _, controlTableUpdateOptions = Map(ProductMaxIdType -> CourseCurriculumKey)))
      .toList
  }
}

object CourseCurriculumHelper {

  def extractCurriculums(coursePublished: Option[DataFrame]): Option[DataFrame] = {
    val courseCurriculumsPublished: Option[DataFrame] =
      coursePublished
        .flatMap(_.withColumn("curriculums", explode(col("curriculums"))).checkEmptyDf)
        .map(_.select("eventType", "id", "occurredOn", "curriculums.*"))
    courseCurriculumsPublished
  }

  def getCombinedCurriculums(session: SparkSession,
                             curriculumsPublished: Option[DataFrame],
                             detailsUpdated: Option[DataFrame]): Option[DataFrame] = {

    val courseCurriculumsUpdatedDF: Option[DataFrame] = getCurriculumUpdatedDF(session, detailsUpdated)

    curriculumsPublished.unionOptionalByNameWithEmptyCheck(courseCurriculumsUpdatedDF)
  }

  private def getCurriculumUpdatedDF(session: SparkSession, detailsUpdated: Option[DataFrame]) = {

    import session.implicits._
    val courseCurriculumsUpdated: Option[DataFrame] =
      if (detailsUpdated.flatMap(_.checkEmptyDf).isEmpty) {
        detailsUpdated
      } else {
        val courseDetailsUpdatedDF = detailsUpdated
          .flatMap(_.filter(size($"curriculums") > 0 and $"courseStatus" === "PUBLISHED").checkEmptyDf)
          .map(
            _.withColumn("curriculum", explode(col("curriculums")))
              .select("eventType", "id", "occurredOn", "curriculum.*")
          )

        val courseWithEmptyCurriculumsUpdated = detailsUpdated.map(
          _.filter(size($"curriculums") === 0 and $"courseStatus" === "PUBLISHED")
            .select("eventType", "id", "occurredOn")
            .withColumn("curriculumId", lit(null).cast(LongType))
            .withColumn("gradeId", lit(null).cast(LongType))
            .withColumn("subjectId", lit(null).cast(LongType)))
        courseDetailsUpdatedDF.unionOptionalByNameWithEmptyCheck(courseWithEmptyCurriculumsUpdated)
      }
    courseCurriculumsUpdated
  }
}

object CourseCurriculumMutatedTransform {
  private val CourseCurriculumMutatedTransformService = "course-curriculum-mutated-transform"

  private val CourseCurriculumKey = "dim_course_curriculum_association"
  val CoursePublishedParquetSource = "parquet-course-published-source"
  val CourseDetailsUpdatedParquetSource = "parquet-course-details-updated-source"

  val CoursePublishedEvent = "CoursePublishedEvent"
  val CourseSettingUpdatedEvent = "CourseSettingsUpdatedEvent"

  val CourseCurriculumMutatedTransformedSink = "parquet-course-curriculum-mutated-transformed-sink"

  val CourseCurriculumEntityPrefix = "cc"
  val CourseCurriculumCols: Map[String, String] = Map[String, String](
    "id" -> "cc_course_id",
    "curriculumId" -> "cc_curr_id",
    "gradeId" -> "cc_curr_grade_id",
    "subjectId" -> "cc_curr_subject_id",
    "cc_status" -> "cc_status",
    "occurredOn" -> "occurredOn"
  )

  val session: SparkSession = SparkSessionUtils.getSession(CourseCurriculumMutatedTransformService)
  val service = new SparkBatchService(CourseCurriculumMutatedTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new CourseCurriculumMutatedTransform(session, service)
    service.runAll(transformer.transform())
  }
}
