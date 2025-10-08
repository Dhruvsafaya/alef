package com.alefeducation.facts.pathway.teacher_activity

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.pathway.teacher_activity.PathwayTeacherActivityTransform._
import com.alefeducation.schema.Schema.schema
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.schema.pathways.{AdditionalResourcesAssignedEvent, DueDate}
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{getNestedString, getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit, regexp_extract, regexp_replace, when}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, TimestampType}

class PathwayTeacherActivityTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {
    val assignedSourceName = getNestedString(PathwayTeacherActivityTransformService, "assigned-source")
    val additionalResourceAssignedSourceName = getNestedString(PathwayTeacherActivityTransformService, "additional-resource-assigned-source")
    val additionalResourceUnassignedSourceName = getNestedString(PathwayTeacherActivityTransformService, "additional-resource-unassigned-source")
    val unassignedSourceName = getNestedString(PathwayTeacherActivityTransformService, "unassigned-source")
    val sinkName = getSink(PathwayTeacherActivityTransformService).head

    val assignedSource = service.readOptional(assignedSourceName, session)
    val unassignedSource = service.readOptional(unassignedSourceName, session)
    val additionalResourceAssignedSource = service.readOptional(additionalResourceAssignedSourceName, session)
    val additionalResourceUnassignedSource = service.readOptional(additionalResourceUnassignedSourceName, session)

    val additionalResourceAssignedDf = additionalResourceAssignedSource.map(
      _.select(col("*"), explode(col("resourceInfo")))
        .withColumn("activityId", col("col.activityId"))
        .withColumn("activityTypeValue", col("col.activityType"))
        .withColumn(s"${PathwayTeacherActivityEntity}_action_name", lit(Assign).cast(IntegerType))
        .withColumn("progressStatus", col("col.progressStatus"))
        .withColumn("startDate", col("dueDate.startDate").cast(DateType))
        .withColumn("endDate", col("dueDate.endDate").cast(DateType))
        .withColumn("isAddedAsResource", lit(true))
        .withColumn("classId", lit(null).cast(StringType))
        .withColumn("levelId", lit(null).cast(StringType))
        .transform(addActivityType)
        .transformForInsertFact(PathwayAdditionalResourceTeacherActivityCols, PathwayTeacherActivityEntity)
    )

    val additionalResourceUnassignedDf = additionalResourceUnassignedSource.map(
      _.withColumn(s"${PathwayTeacherActivityEntity}_action_name", lit(UnAssign).cast(IntegerType))
        .withColumnRenamed("unAssignedBy", "assignedBy")
        .withColumnRenamed("unAssignedOn", "assignedOn")
        .withColumn("activityType", lit(UnknownActivityTYpe))
        .withColumn("progressStatus", lit(null).cast(StringType))
        .withColumn("startDate", lit(null).cast(DateType))
        .withColumn("endDate", lit(null).cast(DateType))
        .withColumn("isAddedAsResource", lit(true))
        .withColumn("classId", lit(null).cast(StringType))
        .withColumn("levelId", lit(null).cast(StringType))
        .withColumn("activityTypeValue", lit(null).cast(StringType))
        .transformForInsertFact(PathwayAdditionalResourceTeacherActivityCols, PathwayTeacherActivityEntity)
    )

    val assignedDF = assignedSource.map(
         _.withColumn(s"${PathwayTeacherActivityEntity}_action_name", lit(Assign).cast(IntegerType))
           .select(col("*"), explode(col("activitiesProgressStatus")))
           .withColumn("activityId", col("col.activityId"))
           .withColumn("activityTypeValue", col("col.activityType"))
           .transform(addActivityType)
           .withColumn("progressStatus", col("col.progressStatus"))
           .withColumn("startDate", col("dueDate.startDate").cast(DateType))
           .withColumn("endDate", col("dueDate.endDate").cast(DateType))
           .withColumn("isAddedAsResource", lit(false))
          .transformForInsertFact(PathwayTeacherActivityCols, PathwayTeacherActivityEntity)
    )

    val unassignedDf = unassignedSource.map(
      _.withColumn(s"${PathwayTeacherActivityEntity}_action_name", lit(UnAssign).cast(IntegerType))
        .withColumnRenamed("unAssignedBy", "assignedBy")
        .withColumnRenamed("unAssignedOn", "assignedOn")
        .withColumn("activityType", lit(UnknownActivityTYpe))
        .withColumn("activityTypeValue", lit(null).cast(StringType))
        .withColumn("progressStatus", lit(null).cast(StringType))
        .withColumn("startDate", lit(null).cast(DateType))
        .withColumn("endDate", lit(null).cast(DateType))
        .withColumn("isAddedAsResource", lit(false))
        .transformForInsertFact(PathwayTeacherActivityCols, PathwayTeacherActivityEntity)
    )

    val startId = service.getStartIdUpdateStatus(PathwayTeacherActivityKey)
    val combinedDf = combineOptionalDfs(assignedDF, unassignedDf, additionalResourceAssignedDf, additionalResourceUnassignedDf)
      .map(
        _.genDwId(s"${PathwayTeacherActivityEntity}_dw_id", startId, s"${PathwayTeacherActivityEntity}_created_time")
          .withColumn(s"${PathwayTeacherActivityEntity}_action_time",
            regexp_replace(col(s"${PathwayTeacherActivityEntity}_action_time"), "T", " ").cast(TimestampType)
          )
      )

    combinedDf.map(DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> PathwayTeacherActivityKey)))
  }

  def addActivityType(df: DataFrame): DataFrame = {
    df.withColumn("activityType", when(col("col.activityType") === "ACTIVITY", lit(LoActivityType))
      .when(col("col.activityType") === "INTERIM_CHECKPOINT", lit(IcActivityType))
      .otherwise(lit(UnknownActivityTYpe)))
  }
}

object PathwayTeacherActivityTransform {
  val PathwayTeacherActivityTransformService = "pathway-teacher-activity-transform"
  val PathwayTeacherActivityEntity = "fpta"

  val PathwayTeacherActivityKey = "fact_pathway_teacher_activity"

  val Assign = 1
  val UnAssign = 2

  val LoActivityType = 1
  val IcActivityType = 2
  val UnknownActivityTYpe = -1

  val PathwayAdditionalResourceTeacherActivityCols: Map[String, String] = Map(
    "tenantId" -> s"${PathwayTeacherActivityEntity}_tenant_id",
    "classId" -> s"${PathwayTeacherActivityEntity}_class_id",
    "levelId" -> s"${PathwayTeacherActivityEntity}_level_id",
    "learnerId" -> s"${PathwayTeacherActivityEntity}_student_id",
    "activityId" -> s"${PathwayTeacherActivityEntity}_activity_id",
    "assignedBy" -> s"${PathwayTeacherActivityEntity}_teacher_id",
    "courseId" -> s"${PathwayTeacherActivityEntity}_pathway_id",
    "assignedOn" -> s"${PathwayTeacherActivityEntity}_action_time",
    "startDate" -> s"${PathwayTeacherActivityEntity}_start_date",
    "endDate" -> s"${PathwayTeacherActivityEntity}_end_date",
    "activityType" -> s"${PathwayTeacherActivityEntity}_activity_type",
    "progressStatus" -> s"${PathwayTeacherActivityEntity}_activity_progress_status",
    "isAddedAsResource" -> s"${PathwayTeacherActivityEntity}_is_added_as_resource",
    s"${PathwayTeacherActivityEntity}_action_name" -> s"${PathwayTeacherActivityEntity}_action_name",
    "activityTypeValue" -> s"${PathwayTeacherActivityEntity}_activity_type_value",
    "occurredOn" -> "occurredOn"
  )

  val PathwayTeacherActivityCols: Map[String, String] = Map(
    "tenantId" -> s"${PathwayTeacherActivityEntity}_tenant_id",
    "classId" -> s"${PathwayTeacherActivityEntity}_class_id",
    "studentId" -> s"${PathwayTeacherActivityEntity}_student_id",
    "levelId" -> s"${PathwayTeacherActivityEntity}_level_id",
    "activityId" -> s"${PathwayTeacherActivityEntity}_activity_id",
    "assignedBy" -> s"${PathwayTeacherActivityEntity}_teacher_id",
    "pathwayId" -> s"${PathwayTeacherActivityEntity}_pathway_id",
    "assignedOn" -> s"${PathwayTeacherActivityEntity}_action_time",
    "assignedOn" -> s"${PathwayTeacherActivityEntity}_action_time",
    "startDate" -> s"${PathwayTeacherActivityEntity}_start_date",
    "endDate" -> s"${PathwayTeacherActivityEntity}_end_date",
    "activityType" -> s"${PathwayTeacherActivityEntity}_activity_type",
    "progressStatus" -> s"${PathwayTeacherActivityEntity}_activity_progress_status",
    "isAddedAsResource" -> s"${PathwayTeacherActivityEntity}_is_added_as_resource",
    s"${PathwayTeacherActivityEntity}_action_name" -> s"${PathwayTeacherActivityEntity}_action_name",
    "activityTypeValue" -> s"${PathwayTeacherActivityEntity}_activity_type_value",
    "occurredOn" -> "occurredOn"
  )

  val session: SparkSession = SparkSessionUtils.getSession(PathwayTeacherActivityTransformService)
  val service = new SparkBatchService(PathwayTeacherActivityTransformService, session)
  def main(args: Array[String]): Unit = {
    val transformer = new PathwayTeacherActivityTransform(session, service)
    service.run(transformer.transform())
  }
}
