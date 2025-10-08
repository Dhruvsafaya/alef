package com.alefeducation.dimensions.class_.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Alias, Delta}
import com.alefeducation.dimensions.class_.delta.ClassStudentUserDelta.{studentUserSinkName, studentUserSourceName}
import com.alefeducation.dimensions.class_.transform.ClassUserTransform.classUserTransformSink
import com.alefeducation.util.Helpers.{ClassUserEntity, DeltaClassUserSink, Student}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
class ClassStudentUserDelta(val session: SparkSession, val service: SparkBatchService) {

  import Delta.Transformations._
  import session.implicits._
  def transform(): Option[Sink] = {
    val classUser = service.readOptional(studentUserSourceName, session)

    val classUserTransformed = classUser.map(
      _.drop("action")
        .withColumnRenamed("class_uuid", "class_id")
        .withColumnRenamed("user_uuid", "user_id")
        .withColumnRenamed("role_uuid", "role_id")
    )
    val enrolledStudents = classUserTransformed.map(_.filter($"role_id" === Student))

    val matchCondition = s"${Alias.Delta}.class_id = ${Alias.Events}.class_id and " +
      s"${Alias.Delta}.user_id = ${Alias.Events}.user_id " +
      s"and ${Alias.Delta}.role_id = 'STUDENT' "
    enrolledStudents.map(
      _.drop($"eventType")
        .toSCDContext(uniqueIdColumns = List("class_id", "user_id"), matchConditions = matchCondition)
        .toSink(studentUserSinkName, ClassUserEntity)
    )
  }
}
object ClassStudentUserDelta {
  val studentUserSourceName: String = classUserTransformSink
  val studentUserSinkName: String = DeltaClassUserSink
  private val studentUserServiceName = "delta-class-student-user"

  private val studentUserSession: SparkSession = SparkSessionUtils.getSession(studentUserServiceName)
  private val studentUserService = new SparkBatchService(studentUserServiceName, studentUserSession)

  def main(args: Array[String]): Unit = {
    val transformer = new ClassStudentUserDelta(studentUserSession, studentUserService)
    studentUserService.run(transformer.transform())
  }

}
