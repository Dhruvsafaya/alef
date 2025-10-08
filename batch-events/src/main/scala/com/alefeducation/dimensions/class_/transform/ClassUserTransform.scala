package com.alefeducation.dimensions.class_.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.class_.transform.ClassUserTransform.{classUserKey, classUserTransformSink}
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.{ClassCreatedEvent, StudentEnrolledInClassEvent}
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}

class ClassUserTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._
  import session.implicits._

  def transform(): Option[Sink] = {
    val classModifiedSource = service.readOptional(ParquetClassModifiedSource, session).map(_.cache())
    val studentClassSource = service.readOptional(ParquetStudentClassAssociationSource, session)

    val teacherCols = ClassUserCols + ("class_user_active_until" -> "class_user_active_until")
    val teacher = classModifiedSource.map(
      _.transformIfNotEmpty(transformForTeacher)
        .transformForInsertOrUpdate(teacherCols, ClassUserEntity)
    )

    val student = studentClassSource.map(
      _.transformIfNotEmpty(transformForStudent)
        .transformForSCD(ClassUserCols, ClassUserEntity, ids = List("classId", "userId"))
    )

    val startId = service.getStartIdUpdateStatus(classUserKey)

    val userSink = combineOptionalDfs(teacher, student).map(
      _.withColumn("class_user_attach_status", when(col("action") === Enroll, lit(EnrollStatus)).otherwise(UnEnrollStatus))
        .drop("action")
        .withColumn("class_user_status", when($"class_user_active_until".isNotNull, Inactive).otherwise(lit(ActiveEnabled)))
        .genDwId("rel_class_user_dw_id", startId, orderByField = "class_user_created_time")
    )

    userSink.map(
      DataSink(classUserTransformSink, _, controlTableUpdateOptions = Map(ProductMaxIdType -> classUserKey))
    )
  }

  private def transformForTeacher(classModified: DataFrame): DataFrame = {
    val latestCreated = classModified
      .filter($"eventType" === ClassCreatedEvent)
      .checkEmptyDf
      .map(_.selectLatestByRowNumber(List("classId")))
    val updated = classModified.filter($"eventType" =!= ClassCreatedEvent).checkEmptyDf
    // We can safely use get here as the method accepts non optional DataFrame
    val latestDf = latestCreated.unionOptionalByName(updated).get

    val w = Window.partitionBy($"classId").orderBy(desc("occurredOn"))
    val latestWithActiveUntilDf = latestDf
      .withColumn("class_user_active_until", lit(NULL).cast(TimestampType))
      .withColumn("class_user_active_until", lag("occurredOn", 1).over(w).cast(TimestampType))

    latestWithActiveUntilDf
      .select($"*", explode_outer($"teachers").as("userId"))
      .select("classId", "userId", "occurredOn", "eventType", "class_user_active_until")
      .withColumn("role", lit(Teacher))
      .withColumn("action", when(col("userId").isNotNull, lit(Enroll)).otherwise(UnEnroll))
  }

  private def transformForStudent(studentClass: DataFrame): DataFrame = {
    studentClass
      .select("classId", "userId", "occurredOn", "eventType")
      .withColumn("role", lit(Student))
      .withColumn("action", when(col("eventType") === StudentEnrolledInClassEvent, lit(Enroll)).otherwise(UnEnroll))
  }

}

object ClassUserTransform {
  val classUserTransformService = "transform-class-user"
  val classUserTransformSink = "transformed-class-user-sink"

  val classUserKey = "dim_class_user"

  private val session: SparkSession = SparkSessionUtils.getSession(classUserTransformService)
  private val service = new SparkBatchService(classUserTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new ClassUserTransform(session, service)
    service.run(transformer.transform())
  }

}
