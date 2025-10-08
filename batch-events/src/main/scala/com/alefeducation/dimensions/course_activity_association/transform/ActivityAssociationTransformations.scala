package com.alefeducation.dimensions.course_activity_association.transform

import com.alefeducation.dimensions.course_activity_container.transform.ContainerMutatedTransform
import com.alefeducation.dimensions.course_activity_association.transform.CourseActivityAssociationTransform.CourseActivityAssociationEntity
import com.alefeducation.schema.ccl.ActivityPlannedInCourseSchema
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

object ActivityAssociationTransformations {
  def dropActivityDuplicates(entityPrefix: String)(activity: DataFrame): DataFrame = activity.dropDuplicates(
    List("courseId", s"${entityPrefix}_activity_id", "courseVersion")
  )
  def createEmptyActivity(implicit session: SparkSession): DataFrame = {
    import session.implicits._

    val nullStr = F.lit(null).cast(StringType)
    Seq
      .empty[ActivityPlannedInCourseSchema]
      .toDF()
      .withColumn("eventType", nullStr)
      .withColumn("loadtime", nullStr)
      .withColumn("eventDateDw", nullStr)
  }

  def castIdToStr(df: DataFrame): DataFrame = df.withColumn("id", F.col("id").cast(StringType))

  def selectRequiredColumns(columns: List[String])(df: DataFrame): DataFrame = df.select(columns.head, columns.tail: _*)

  def addActivityId(entityPrefix: String, activityFieldName: String)(df: DataFrame): DataFrame =
    df.withColumn(s"${entityPrefix}_activity_id", F.col(activityFieldName)).drop(activityFieldName)

  def addActivityType(status: Int)(df: DataFrame): DataFrame =
    df.withColumn(s"${CourseActivityAssociationEntity}_activity_type", F.lit(status))

  def addSettingsOrDefaultValue(df: DataFrame): DataFrame = {
    if (df.columns.contains("settings")) {
      df.withColumn(
          s"${CourseActivityAssociationEntity}_activity_pacing",
          when(col("settings.pacing").isNotNull, col("settings.pacing")).otherwise(null).cast(StringType)
        )
        .withColumn(
          s"${CourseActivityAssociationEntity}_activity_is_optional",
          when(col("settings.isOptional").isNotNull, col("settings.isOptional")).otherwise(null).cast(BooleanType)
        )
        .drop("settings")
    } else {
      df.withColumn(s"${CourseActivityAssociationEntity}_activity_pacing", lit(null).cast(StringType))
        .withColumn(s"${CourseActivityAssociationEntity}_activity_is_optional", lit(null).cast(BooleanType))
    }
  }

  def addIsParentDeleted(df: DataFrame): DataFrame = {
    val isParentDeletedCol = if (df.columns.contains("isParentDeleted")) {
      F.col("isParentDeleted")
    } else {
      F.lit(false).cast(BooleanType)
    }
    df.withColumn(s"${CourseActivityAssociationEntity}_is_parent_deleted", isParentDeletedCol)
      .drop("isParentDeleted")
  }

  def addIsJointParentActivity(df: DataFrame): DataFrame = {
    val column = if (df.columns.contains("isJointParentActivity")) {
      F.col("isJointParentActivity")
    } else {
      F.lit(null).cast(BooleanType)
    }
    df.withColumn(s"${CourseActivityAssociationEntity}_is_joint_parent_activity", column)
      .drop("isJointParentActivity")
  }

  def addGrade(gradeColumnName: String, spark: SparkSession)(df: DataFrame): DataFrame = {

    /**
      * Find a `metadata.tags` element which has a `key` == "Grade".
      * Extract `values` from the element and explode.
      * If `metadata.tags` is empty or no Grade `key` found, then set pathway_level_grade as null
      */
    import com.alefeducation.util.BatchTransformerUtility._
    import spark.implicits._

    if (df.isEmpty) {
      return df.withColumn(gradeColumnName, F.lit(null).cast(StringType))
    }

    val dfIsEmptyMetadataFlag: DataFrame = df.withColumn(
      "is_metadata_empty",
      F.when(F.size(F.col("metadata.tags")) === 0, F.lit(true)).otherwise(F.lit(false))
    )

    val dfNonEmptyMetadata: Option[DataFrame] = dfIsEmptyMetadataFlag
      .filter($"is_metadata_empty" === false)
      .drop("is_metadata_empty")
      .checkEmptyDf

    val dfGrade: Option[DataFrame] = dfNonEmptyMetadata.map(
      _.withColumn("grade_index", F.array_position(F.col("metadata.tags.key"), "Grade").cast(IntegerType))
        .withColumn(
          "grades_tag",
          F.when($"grade_index" === 0, F.lit(null)).otherwise(F.element_at(F.col(s"metadata.tags"), $"grade_index"))
        )
        .withColumn("grade_values", $"grades_tag.values")
        .withColumn(gradeColumnName, F.explode_outer($"grade_values"))
        .drop("grade_index", "grades_tag", "grade_values")
    )

    val dfEmptyMetadata: Option[DataFrame] = dfIsEmptyMetadataFlag
      .filter($"is_metadata_empty" === true)
      .withColumn(gradeColumnName, F.lit(null).cast(StringType))
      .drop("is_metadata_empty")
      .checkEmptyDf

    dfEmptyMetadata.unionOptionalByNameWithEmptyCheck(dfGrade).get
  }

  def addEmptyGrade(df: DataFrame): DataFrame =
    df.withColumn(s"${CourseActivityAssociationEntity}_grade", lit(null).cast(StringType))

  def addEmptyMetadata(df: DataFrame): DataFrame =
    df.withColumn(s"${CourseActivityAssociationEntity}_metadata", lit(null).cast(ContainerMutatedTransform.metadataSchema))

}
