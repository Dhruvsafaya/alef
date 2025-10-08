package com.alefeducation.dimensions.class_.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.operations.DeltaSCDSink
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.dimensions.class_.transform.ClassModifiedTransform.classModifiedTransformSink
import com.alefeducation.dimensions.class_.transform.ClassUserTransform.classUserTransformSink
import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class ClassDeltaSpec extends SparkSuite with BaseDimensionSpec {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val userTransformedValue: String = """
                               |[
                               |{
                               |  "eventType": "ClassCreatedEvent",
                               |  "class_uuid": "class1",
                               |  "user_uuid": "user1",
                               |  "role_uuid": "TEACHER",
                               |  "action": "say hi",
                               |  "occurredOn": "2000-01-01 01:00:00.0"
                               |},
                               |{
                               |  "eventType": "StudentEnrolledInClassEvent",
                               |  "class_uuid": "class2",
                               |  "user_uuid": "user2",
                               |  "role_uuid": "STUDENT",
                               |  "action": "say hi",
                               |  "occurredOn": "2000-01-01 02:00:00.0"
                               |}
                               |]""".stripMargin
  val userDeltaExpectedColumns: Set[String] = Set("class_id", "user_id", "role_id", "occurredOn")

  test("Should transform class modified to save into Delta") {
    val someValue = """
              |{
              |  "eventType": "ClassModifiedEvent",
              |  "class_id": "grade-id",
              |  "occurredOn": "2000-01-01 01:00:00.0"
              |}
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(someValue).toDS())

    val transformer = new ClassModifiedDelta(sprk, service)
    when(service.readOptional(classModifiedTransformSink, sprk)).thenReturn(Some(inputDF))

    val sink = transformer.transform().get.asInstanceOf[DeltaSCDSink]

    val expectedMatchConditions =
      """
        |delta.class_id = events.class_id and delta.class_created_time <= events.class_created_time
        |""".stripMargin
    replaceSpecChars(sink.matchConditions) shouldBe replaceSpecChars(expectedMatchConditions)

    val expectedUpdateFields = Map("class_active_until" -> "events.class_created_time")
    assert(sink.updateFields === expectedUpdateFields)
  }

  test("Should transform class teacher user to save into Delta") {
    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(userTransformedValue).toDS())

    val transformer = new ClassTeacherUserDelta(sprk, service)
    when(service.readOptional(classUserTransformSink, sprk)).thenReturn(Some(inputDF))

    val sink = transformer.transform().get.asInstanceOf[DeltaSCDSink]

    assert(sink.df.columns.toSet === userDeltaExpectedColumns)
    assert(sink.df.count() == 1)
    assert[String](sink.df, "class_id", "class1")

    val expectedMatchConditions =
      """
        |delta.class_id = events.class_id
        | and delta.role_id = 'TEACHER'
        | and delta.class_user_created_time <= events.class_user_created_time
        |""".stripMargin
    replaceSpecChars(sink.matchConditions) shouldBe replaceSpecChars(expectedMatchConditions)

    val expectedUpdateFields = Map("class_user_active_until" -> "events.class_user_created_time")
    assert(sink.updateFields === expectedUpdateFields)

    val expectedUniqueIdColumns = List("class_id")
    assert(sink.uniqueIdColumns === expectedUniqueIdColumns)
  }

  test("Should transform class student user to save into Delta") {
    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(userTransformedValue).toDS())

    val transformer = new ClassStudentUserDelta(sprk, service)
    when(service.readOptional(classUserTransformSink, sprk)).thenReturn(Some(inputDF))

    val sink = transformer.transform().get.asInstanceOf[DeltaSCDSink]

    assert(sink.df.columns.toSet === userDeltaExpectedColumns)
    assert(sink.df.count() == 1)
    assert[String](sink.df, "class_id", "class2")

    val expectedMatchConditions =
      """
        |delta.class_id = events.class_id
        | and delta.user_id = events.user_id
        | and delta.role_id = 'STUDENT'
        | and delta.class_user_created_time <= events.class_user_created_time
        |""".stripMargin
    replaceSpecChars(sink.matchConditions) shouldBe replaceSpecChars(expectedMatchConditions)

    val expectedUpdateFields = Map("class_user_active_until" -> "events.class_user_created_time")
    assert(sink.updateFields === expectedUpdateFields)

    val expectedUniqueIdColumns = List("class_id", "user_id")
    assert(sink.uniqueIdColumns === expectedUniqueIdColumns)
  }

}
