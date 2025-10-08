package com.alefeducation.facts.guardian_joint_activity

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.guardian_joint_activity.GuardianJointActivityTransform.{GuardianJointActivityCols, GuardianJointActivityColsMapping, GuardianJointActivityCompletedSource, GuardianJointActivityEntity, GuardianJointActivityPendingSource, GuardianJointActivityRatedSource, GuardianJointActivityStartedSource, GuardianJointActivityTransformService, GuardianJointActivityTransformedSink}
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

class GuardianJointActivityTransform(val session: SparkSession, val service: SparkBatchService){
  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {

    import com.alefeducation.util.BatchTransformerUtility._

    val pendingSource = service.readOptional(GuardianJointActivityPendingSource, session, extraProps = List(("mergeSchema", "true")))
    val pendingTransformed = pendingSource.map(
      _.withColumn("guardianId", explode(col("guardianIds"))).drop("guardianIds")
      .withColumn("fgja_state", lit(1))
      .transform(addColsIfNotExists())
    )

    val startedSource = service.readOptional(GuardianJointActivityStartedSource, session, extraProps = List(("mergeSchema", "true")))
    val startedTransformed = startedSource.map(
      _.withColumnRenamed("startedByGuardianId", "guardianId")
      .withColumn("fgja_state", lit(2))
      .transform(addColsIfNotExists()).select(GuardianJointActivityCols.map(m=>col(m)):_*)
    )

    val completedSource = service.readOptional(GuardianJointActivityCompletedSource, session, extraProps = List(("mergeSchema", "true")))
    val completedTransformed = completedSource.map(
      _.withColumnRenamed("completedByGuardianId", "guardianId")
        .withColumn("fgja_state", lit(3))
        .transform(addColsIfNotExists()).select(GuardianJointActivityCols.map(m=>col(m)):_*)
    )

    val ratedSource = service.readOptional(GuardianJointActivityRatedSource, session, extraProps = List(("mergeSchema", "true")))
    val ratedTransformed = ratedSource.map(
      _.withColumn("fgja_state", lit(4)).select(GuardianJointActivityCols.map(m=>col(m)):_*)
    )

    val allActivitiesTransformed = pendingTransformed.unionOptionalByNameWithEmptyCheck(startedTransformed).unionOptionalByNameWithEmptyCheck(completedTransformed).unionOptionalByNameWithEmptyCheck(ratedTransformed)

    val allActivitiesToFact = allActivitiesTransformed.map(
      _.transformForInsertFact(GuardianJointActivityColsMapping, GuardianJointActivityEntity)
    )

    allActivitiesToFact.map(DataSink(GuardianJointActivityTransformedSink, _))
  }

  def addColsIfNotExists(): DataFrame => DataFrame =
    _.addColIfNotExists("rating", IntegerType)
      .addColIfNotExists("attempt", IntegerType)

}

object GuardianJointActivityTransform {
  val GuardianJointActivityTransformService = "transform-guardian-joint-activity"
  val GuardianJointActivityPendingSource = "parquet-guardian-joint-activity-pending-source"
  val GuardianJointActivityStartedSource = "parquet-guardian-joint-activity-started-source"
  val GuardianJointActivityCompletedSource = "parquet-guardian-joint-activity-completed-source"
  val GuardianJointActivityRatedSource = "parquet-guardian-joint-activity-rated-source"
  val GuardianJointActivityTransformedSink = "transformed-guardian-joint-activity-sink"
  val GuardianJointActivityEntity = "fgja"

  val GuardianJointActivityCols: List[String] = List(
    "pathwayId",
    "pathwayLevelId",
    "classId",
    "studentId",
    "schoolId",
    "guardianId",
    "grade",
    "attempt",
    "rating",
    "occurredOn",
    "tenantId",
    "fgja_state",
    "eventType",
    "loadtime",
    "eventDateDw"
  )

  val GuardianJointActivityColsMapping: Map[String, String] = Map(
    "pathwayId" -> "fgja_pathway_id",
    "pathwayLevelId" -> "fgja_pathway_level_id",
    "classId" -> "fgja_class_id",
    "studentId" -> "fgja_student_id",
    "schoolId" -> "fgja_school_id",
    "guardianId" -> "fgja_guardian_id",
    "grade" -> "fgja_k12_grade",
    "attempt" -> "fgja_attempt",
    "rating" -> "fgja_rating",
    "occurredOn" -> "occurredOn",
    "tenantId" -> "fgja_tenant_id",
    "fgja_state" -> "fgja_state"
  )


  val session = SparkSessionUtils.getSession(GuardianJointActivityTransformService)
  val service = new SparkBatchService(GuardianJointActivityTransformService, session)

  def main(args: Array[String]): Unit = {
    val transform = new GuardianJointActivityTransform(session, service)
    service.run(transform.transform())
  }
}
