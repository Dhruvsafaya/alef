package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch.Consts
import com.alefeducation.bigdata.commons.testutils.{ExpectedFields, SparkSuite}
import org.apache.spark.sql.types.StringType
import org.scalatest.matchers.must.Matchers
import org.scalatest.{Inside, OptionValues}

class DeltaDeleteTests extends SparkSuite with Matchers with Inside with OptionValues {

  import ExpectedFields._

  test("Delta.Transformations.DeltaDataFrameTransformations.toDelete :: empty data frame") {
    import Delta.Transformations.DeltaDataFrameTransformations

    val df = spark.emptyDataFrame
    val sut = df.toDelete()

    sut mustBe empty
  }

  test("Delta.Transformations.DeltaDataFrameTransformations.toDelete :: do not transform") {
    import Delta.Transformations.DeltaDataFrameTransformations
    val s = spark
    import s.sqlContext.implicits._

    val df = List(Tuple1("someValue")).toDF("someColumn")
    val sut = df.toDelete()

    sut must contain(DeleteContext(df = df, matchConditions = ""))
  }

  test("Delta.Transformations.DeltaDataFrameTransformations.toDelete :: transform") {
    import Delta.Transformations.DeltaDataFrameTransformations
    val s = spark
    import s.sqlContext.implicits._

    val row = ("someUUID", "SomeDeletedEvent", "2020-05-12 07:10:03.841", "someFieldValue")
    val df = List(row).toDF("uuid", "eventType", "occurredOn", "someField")
    val entity = "testentity"
    val transform = Transform(entity = entity)
    val sut = df.toDelete(Some(transform))

    inside(sut.value) {
      case DeleteContext(outputDF, "") =>
        val expectedFields = List(
          ExpectedField(name = s"${entity}_uuid", dataType = StringType),
          ExpectedField(name = s"${entity}_some_field", dataType = StringType)
        ) ++ DeltaTests.ExpectedFields.notFact(entity)
        assertExpectedFields(outputDF.schema.fields.toList, expectedFields)
    }
  }

  test("Delta.Transformations.DeltaDeleteContextTransformations.toSink") {
    import Delta.Transformations.DeltaDeleteContextTransformations

    val df = spark.emptyDataFrame
    val sinkName = "someSink"
    val entity = "testentity"
    val sut = DeleteContext(df = df, matchConditions = "").toSink(sinkName, entity)

    sut.df mustBe df
    sut.name mustBe sinkName
    sut.input mustBe df
    sut.updateFields mustBe Map(
      s"${entity}_updated_time" -> s"${Alias.Events}.${entity}_created_time",
      s"${entity}_dw_updated_time" -> s"${Alias.Events}.${entity}_dw_created_time",
      s"${entity}_deleted_time" -> s"${Alias.Events}.${entity}_created_time",
      s"${entity}_status" -> Consts.Deleted.toString
    )
  }

}
