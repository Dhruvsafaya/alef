package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.commons.testutils.ExpectedFields.ExpectedField
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

object DeltaTests {

  object ExpectedFields {

    def notFact(entity: String): List[ExpectedField] = List(
      ExpectedField(name = s"${entity}_status", dataType = IntegerType),
      ExpectedField(name = s"${entity}_created_time", dataType = TimestampType),
      ExpectedField(name = s"${entity}_dw_created_time", dataType = TimestampType),
      ExpectedField(name = s"${entity}_updated_time", dataType = TimestampType),
      ExpectedField(name = s"${entity}_deleted_time", dataType = TimestampType),
      ExpectedField(name = s"${entity}_dw_updated_time", dataType = TimestampType)
    )

    def fact(entity: String): List[ExpectedField] = List(
      ExpectedField(name = "eventdate", dataType = StringType),
      ExpectedField(name = s"${entity}_date_dw_id", dataType = StringType),
      ExpectedField(name = s"${entity}_created_time", dataType = TimestampType),
      ExpectedField(name = s"${entity}_dw_created_time", dataType = TimestampType)
    )

  }

}

class DeltaTests extends SparkSuite with Matchers {

  test("DeltaOperation.buildMatchColumns") {
    val entity = "test_entity"
    val conditions = s"${Alias.Delta}.${entity}_test_column = ${Alias.Events}.${entity}_test_column"
    val sut = DeltaOperation.buildMatchColumns(conditions, entity)

    sut must equal(s"""|${conditions} and
          |${Alias.Delta}.${entity}_created_time < ${Alias.Events}.${entity}_created_time""".stripMargin.replaceAll("\n", " "))
  }

  test("DeltaOperation.buildMatchColumns with empty conditions") {
    val entity = "test_entity"
    val conditions = ""
    val sut = DeltaOperation.buildMatchColumns(conditions, entity)

    sut must equal(s"""|${Alias.Delta}.${entity}_id = ${Alias.Events}.${entity}_id and
                       |${Alias.Delta}.${entity}_created_time < ${Alias.Events}.${entity}_created_time""".stripMargin.replaceAll("\n", " "))
  }

  test("Delta.Transformations.applyTransformations :: empty data frame") {
    import Delta.Transformations.applyTransformations

    val operationMock = mock[DeltaOperation]
    val sut = applyTransformations(spark.emptyDataFrame, operationMock, None)

    sut mustBe empty
    verify(operationMock, never()).applyTransformations(ArgumentMatchers.eq(spark.emptyDataFrame), any[Transform])
  }

  test("Delta.Transformations.applyTransformations :: do not transform") {
    import Delta.Transformations.applyTransformations
    val s = spark
    import s.sqlContext.implicits._

    val operationMock = mock[DeltaOperation]
    val df = List(Tuple1("someValue")).toDF("someColumn")
    val sut = applyTransformations(df, operationMock, None)

    sut must contain(df)
    verify(operationMock, never()).applyTransformations(ArgumentMatchers.eq(df), any[Transform])
  }

  test("Delta.Transformations.applyTransformations :: transform") {
    import Delta.Transformations.applyTransformations
    val s = spark
    import s.sqlContext.implicits._

    val df = List(Tuple1("someValue")).toDF("someColumn")
    val transform = Transform(entity = "someEntity")
    val operationMock = mock[DeltaOperation]
    when(operationMock.applyTransformations(df, transform)).thenReturn(df)
    val sut = applyTransformations(df, operationMock, Some(transform))

    sut must contain(df)
  }

}
