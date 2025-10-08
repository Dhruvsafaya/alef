package com.alefeducation.dimensions.teacher

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.teacher.TeacherTransform.{TeacherDimension, TeacherTransformSink}
import com.alefeducation.models.TeacherModel._
import com.alefeducation.schema.admin.TeacherMovedBetweenSchools
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.{Resources, SparkSessionUtils}
import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

class TeacherTransform(val session: SparkSession, val service: SparkBatchService) {

  import session.implicits._

  def transform(): Option[Sink] = {

    val teacherDf = service.readOptional(ParquetTeacherSource, session)
    val teacherDfs = teacherDf.map(
      _.select($"uuid", $"schoolId", $"occurredOn", $"eventType", $"loadtime")
        .withColumn("teacher_status", when($"eventType".contains("Disabled"), lit(Disabled)).otherwise(lit(ActiveEnabled)))
    ).getOrElse(Seq.empty[TeacherMutated].toDF)

    val teacherCreated = teacherDfs.filter($"eventType" === AdminTeacher)
    val teacherEnabledDisabled = teacherDfs.filter($"eventType" === AdminTeacherEnabledEvent || $"eventType" === AdminTeacherDisabledEvent)

    val teacherSchoolMovedDf = service.readOptional(ParquetTeacherSchoolMovedSource, session)
    val teacherSchoolMoved = teacherSchoolMovedDf.getOrElse(Seq.empty[TeacherMovedBetweenSchools].toDF)

    val teacherUuidsDf = teacherCreated
      .select($"uuid".as("teacherId"))
      .unionByName(teacherEnabledDisabled.select($"uuid".as("teacherId")))
      .unionByName(teacherSchoolMoved.select($"teacherId"))
      .distinct
    val teacherIdsList = teacherUuidsDf.select("teacherId").as[String].collect().toList.mkString("'", "','", "'")

    val teacherRSRelSource = readFromRedshiftIfRequired[TeacherRel](teacherIdsList,
      s"""select * from ${Resources.redshiftStageSchema()}.rel_teacher where teacher_id in (${teacherIdsList}) and teacher_active_until is null""".stripMargin
    )
    val teacherRSRelDf = teacherRSRelSource
      .select(
        $"teacher_created_time".as("occurredOn"),
        $"teacher_id".as("uuid"),
        $"school_id".as("schoolId"),
        $"teacher_status"
      ).withColumn("loadtime", lit(DefaultTime).cast(TimestampType))

    val dimSchema = Resources.redshiftSchema()

    val dimTeacher = readFromRedshiftIfRequired[TeacherDim](teacherIdsList,
      s"""select * from ${dimSchema}.dim_teacher where teacher_id in (${teacherIdsList}) and teacher_active_until is null""".stripMargin
    )
    val teacherSchoolIdsList = dimTeacher.select("teacher_school_dw_id").distinct.as[Long].collect().toList.mkString(",")
    val schoolDim = readFromRedshiftIfRequired[SchoolDim](teacherSchoolIdsList,
      s"""select * from ${dimSchema}.dim_school where school_dw_id in (${teacherSchoolIdsList})""".stripMargin
    )

    val teacherRSDimDf = dimTeacher
      .join(schoolDim, col("teacher_school_dw_id") === col("school_dw_id"))
      .select(
        $"teacher_created_time".as("occurredOn"),
        $"teacher_id".as("uuid"),
        $"school_id".as("schoolId"),
        $"teacher_status"
      ).withColumn("loadtime", lit(DefaultTime).cast(TimestampType))

    val teacherFromRSDimRelAndIncoming = teacherRSDimDf
      .unionByName(teacherRSRelDf.drop("eventType"))
      .unionByName(teacherCreated.drop("eventType"))
      .join(teacherUuidsDf, col("uuid") === col("teacherId"), "inner")
      .cache

    val teacherSchoolDf = selectLatestRecordsByRowNumber(
      teacherFromRSDimRelAndIncoming
        .select($"occurredOn".as("occurredOnSchoolMovement"), $"uuid".as("schoolTeacherUuid"), $"schoolId")
        .union(teacherSchoolMoved
          .select($"occurredOn".as("occurredOnSchoolMovement"), $"teacherId".as("schoolTeacherUuid"), $"targetSchoolId".as("schoolId"))),
      List("schoolTeacherUuid"),
      "occurredOnSchoolMovement"
    )

    val teacherStatusDf = selectLatestRecordsByRowNumberOrderByDateCols(
      teacherFromRSDimRelAndIncoming
        .select($"occurredOn".as("occurredOnStatus"), $"uuid".as("statusTeacherUuid"), $"teacher_status", $"loadtime")
        .unionByName(
          teacherEnabledDisabled.select($"occurredOn".as("occurredOnStatus"), $"uuid".as("statusTeacherUuid"), $"teacher_status", $"loadtime")),
      List("statusTeacherUuid"),
      List("occurredOnStatus", "loadtime")
    )

    val teacherCombinedDf =
      teacherSchoolDf
        .join(teacherStatusDf, col("schoolTeacherUuid") === col("statusTeacherUuid"))
        .select(latestOccurredOnTimeUDF(col("occurredOnSchoolMovement"), col("occurredOnStatus")).as(
          "occurredOn"),
          col("*"))
        .withColumn("teacher_active_until", lit(NULL).cast(TimestampType))
        .withColumn("eventType", when(col("teacher_status") === 1, lit(AdminTeacher)).otherwise(AdminTeacherDisabledEvent))

    val teachersWithTimestampsDF = teacherCombinedDf.transformForSCD(TeacherDimensionCols, TeacherEntity)
    Some(DataSink(TeacherTransformSink, teachersWithTimestampsDF))
  }

  private def readFromRedshiftIfRequired[T](ids: String, query: String)(implicit encoder: Encoder[T]) = {
    if (ids.nonEmpty) {
      service.readFromRedshiftQuery[T](query)
    } else Seq.empty[T].toDF
  }

}

object TeacherTransform {
  val TeacherDimension = "dim-teacher"
  val TeacherTransformService = "transform-teacher"
  val TeacherTransformSink = "teacher-transform-rel"

  val session: SparkSession = SparkSessionUtils.getSession(TeacherTransformService)
  val service = new SparkBatchService(TeacherTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new TeacherTransform(session, service)
    service.run(transformer.transform())
  }
}
