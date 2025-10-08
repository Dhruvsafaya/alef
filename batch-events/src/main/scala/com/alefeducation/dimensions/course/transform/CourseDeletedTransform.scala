package com.alefeducation.dimensions.course.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.course.transform.CourseDeletedTransform._
import com.alefeducation.dimensions.course.transform.CourseMutatedTransform.{CourseDeltaSource, CourseEntityPrefix, CourseKey}
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StringType



class CourseDeletedTransform(val session: SparkSession, val service: SparkBatchService) {
  def transform(): List[Option[Sink]] = {

    //Added extra props to use mergeSchema: true property
    val courseDeleted: Option[DataFrame] = service.readUniqueOptional(
      CourseDeletedParquetSource,
      session,
      extraProps = List(("mergeSchema", "true")),
      uniqueColNames = List("id")
    )
    val startId = service.getStartIdUpdateStatus(CourseKey)

    val courseDeletedDefault = courseDeleted.map(df => df.transform(addDefaultColumn))

    val existingCourses: Option[DataFrame] = service.readOptional(CourseDeltaSource, session).map(
      _.addColIfNotExists("course_organization", StringType)
        .addColIfNotExists("course_content_repository", StringType)
        .addColIfNotExists("course_description", StringType)
        .addColIfNotExists("course_goal", StringType)
        .addColIfNotExists("course_name", StringType)
        .addColIfNotExists("course_code", StringType)
    )

    val coursesDetailsDeletedEnriched = courseDeletedDefault
      .map(
        _.filter(col("courseStatus") === "PUBLISHED")
          .joinOptional(existingCourses,
            col("id") === col("course_id")
              && col("course_status") === lit(1),
            "inner")
          .drop("course_status")
      )

    val courseDeletedIWH = coursesDetailsDeletedEnriched.flatMap(
      _.transformForDelete(CourseDeletedCols, CourseEntityPrefix, List("id", "course_content_repository"))
        .genDwId("rel_course_dw_id", startId)
        .checkEmptyDf
    )

    List(
      courseDeletedIWH.map(DataSink(CourseDeletedTransformedSink, _, controlTableUpdateOptions = Map(ProductMaxIdType -> CourseKey))),
    )
  }
}

object CourseDeletedTransform {
  val CourseDeletedTransformService = "course-deleted-transform"

  val CourseDeletedParquetSource = "parquet-course-deleted-source"

  //dim_course
  val CourseDeletedTransformedSink = "parquet-course-deleted-transformed-source"

  val CourseDeletedCols: Map[String, String] = Map[String, String](
    "id" -> "course_id",
    "courseType" -> "course_type",
    "course_status" -> "course_status",
    "course_name" -> "course_name",
    "course_code" -> "course_code",
    "course_organization" -> "course_organization",
    "course_content_repository" -> "course_content_repository",
    "course_description" -> "course_description",
    "course_goal" -> "course_goal",
    "occurredOn" -> "occurredOn"
  )

  val session: SparkSession = SparkSessionUtils.getSession(CourseDeletedTransformService)
  val service = new SparkBatchService(CourseDeletedTransformService, session)
  def addDefaultColumn(df: DataFrame): DataFrame =
    if (df.columns.contains("courseStatus")) df
    else df.withColumn("courseStatus", col("status"))

  def main(args: Array[String]): Unit = {
    val transformer = new CourseDeletedTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }
}
