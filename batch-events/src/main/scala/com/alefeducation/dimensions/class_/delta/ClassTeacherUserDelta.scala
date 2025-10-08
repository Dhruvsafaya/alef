package com.alefeducation.dimensions.class_.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.dimensions.class_.delta.ClassTeacherUserDelta.{classTeacherSinkName, classTeacherSourceName}
import com.alefeducation.dimensions.class_.transform.ClassUserTransform.classUserTransformSink
import com.alefeducation.util.Helpers.{ClassUserEntity, DeltaClassUserSink, Teacher}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class ClassTeacherUserDelta(val session: SparkSession, val service: SparkBatchService) {

  import Delta.Transformations._
  import session.implicits._
  def transform(): Option[Sink] = {
    val classUser = service.readOptional(classTeacherSourceName, session)

    val classUserTransformed = classUser.map(
      _.drop("action")
        .withColumnRenamed("class_uuid", "class_id")
        .withColumnRenamed("user_uuid", "user_id")
        .withColumnRenamed("role_uuid", "role_id")
    )

    val enrolledTeachers = classUserTransformed.map(_.filter($"role_id" === Teacher))

    val matchCondition = s"${Alias.Delta}.class_id = ${Alias.Events}.class_id and ${Alias.Delta}.role_id = 'TEACHER' "
    enrolledTeachers.map(
      _.drop($"eventType")
        .toSCDContext(uniqueIdColumns = List("class_id"), matchConditions = matchCondition)
        .toSink(classTeacherSinkName, ClassUserEntity)
    )
  }
}
object ClassTeacherUserDelta {
  val classTeacherSourceName: String = classUserTransformSink
  val classTeacherSinkName: String = DeltaClassUserSink
  private val classTeacherServiceName = "delta-class-teacher-user"

  private val classTeacherSession: SparkSession = SparkSessionUtils.getSession(classTeacherServiceName)
  private val classTeacherService = new SparkBatchService(classTeacherServiceName, classTeacherSession)

  def main(args: Array[String]): Unit = {
    val transformer = new ClassTeacherUserDelta(classTeacherSession, classTeacherService)
    classTeacherService.run(transformer.transform())
  }

}
