package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch.delta.DeltaUpdateTests.{NestedField, RowWithId, RowWithUUID}
import com.alefeducation.bigdata.commons.testutils.{ExpectedFields, SparkSuite}
import org.apache.spark.sql.types.StringType
import org.scalatest.matchers.must.Matchers
import org.scalatest.{Inside, OptionValues}

object DeltaUpdateTests {

  case class NestedField(nestedField: String)

  case class RowWithUUID(uuid: String, nested: NestedField, notNested: String, eventType: String, occurredOn: String, eventDateDw: String)

  case class RowWithId(id: String, nested: NestedField, notNested: String, eventType: String, occurredOn: String, eventDateDw: String)

}

class DeltaUpdateTests extends SparkSuite with Matchers with Inside with OptionValues {

  import ExpectedFields._

  test("DeltaUpdate.applyTransformations :: default transformations :: not fact") {
    val s = spark
    import s.sqlContext.implicits._

    val firstRow = RowWithUUID(
      uuid = "someUUID",
      nested = NestedField(nestedField = "someFirstNestedFieldValue"),
      notNested = "someFirstNotNestedFieldValue",
      eventType = "SomeUpdatedEvent",
      occurredOn = "2020-05-06 15:00:00.000",
      eventDateDw = "2020-05-06 15:00:00.000"
    )
    val secondRow = RowWithUUID(
      uuid = "someUUID",
      nested = NestedField(nestedField = "someSecondNestedFieldValue"),
      notNested = "someSecondNotNestedFieldValue",
      eventType = "SomeUpdatedEvent",
      occurredOn = "2020-05-06 14:00:00.000",
      eventDateDw = "2020-05-06 14:00:00.000"
    )

    val df = sparkContext.parallelize(List(firstRow, secondRow)).toDF
    val entity = "testentity"
    val transform = Transform(entity = entity)
    val sut = DeltaUpdate.applyTransformations(df, transform)

    val expectedFields = List(
      ExpectedField(name = s"${entity}_uuid", dataType = StringType),
      ExpectedField(name = s"${entity}_nested_nested_field", dataType = StringType),
      ExpectedField(name = s"${entity}_not_nested", dataType = StringType)
    ) ++ DeltaTests.ExpectedFields.notFact(entity)
    assertExpectedFields(sut.schema.fields.toList, expectedFields)
    sut.count() mustBe 1
    val firstDFRow = sut.first()
    firstDFRow.getAs[String](s"${entity}_uuid") mustBe firstRow.uuid
    firstDFRow.getAs[String](s"${entity}_not_nested") mustBe firstRow.notNested

  }

  test("DeltaUpdate.applyTransformations :: custom transformations :: fact") {
    val s = spark
    import s.sqlContext.implicits._

    val firstRow = RowWithId(
      id = "someId",
      nested = NestedField(nestedField = "someFirstNestedFieldValue"),
      notNested = "someFirstNotNestedFieldValue",
      eventType = "SomeUpdatedEvent",
      occurredOn = "2020-05-06 15:00:00.000",
      eventDateDw = "2020-05-06 15:00:00.000"
    )
    val secondRow = RowWithId(
      id = "someId",
      nested = NestedField(nestedField = "someSecondNestedFieldValue"),
      notNested = "someSecondNotNestedFieldValue",
      eventType = "SomeUpdatedEvent",
      occurredOn = "2020-05-06 14:00:00.000",
      eventDateDw = "2020-05-06 14:00:00.000"
    )

    val df = sparkContext.parallelize(List(firstRow, secondRow)).toDF
    val entity = "testentity"
    val transform = Transform(entity = entity,
                              isFact = true,
                              withoutColumns = "not_nested" :: Nil,
                              mappingsOverride = Map("nested_nested_field" -> "overridden_column"),
                              uniqueIdColumns = "id" :: Nil)
    val sut = DeltaUpdate.applyTransformations(df, transform)

    val expectedFields = List(
      ExpectedField(name = s"${entity}_id", dataType = StringType),
      ExpectedField(name = s"${entity}_overridden_column", dataType = StringType)
    ) ++ DeltaTests.ExpectedFields.fact(entity)
    assertExpectedFields(sut.schema.fields.toList, expectedFields)
    sut.count() mustBe 1
    val firstDFRow = sut.first()
    firstDFRow.getAs[String](s"${entity}_id") mustBe firstRow.id
  }

  test("Delta.Transformations.DeltaDataFrameTransformations.toUpdate :: empty data frame") {
    import Delta.Transformations.DeltaDataFrameTransformations

    val df = spark.emptyDataFrame
    val sut = df.toUpdate()

    sut mustBe empty
  }

  test("Delta.Transformations.DeltaDataFrameTransformations.toUpdate :: do not transform") {
    import Delta.Transformations.DeltaDataFrameTransformations
    val s = spark
    import s.sqlContext.implicits._

    val df = List(Tuple1("someValue")).toDF("someColumn")
    val sut = df.toUpdate()

    sut must contain(UpdateContext(df = df, matchConditions = "", updateColumns = Nil))
  }

  test("Delta.Transformations.DeltaDataFrameTransformations.toUpdate :: transform") {
    import Delta.Transformations.DeltaDataFrameTransformations
    val s = spark
    import s.sqlContext.implicits._

    val row = ("someUUID", "SomeUpdatedEvent", "2020-05-12 07:10:03.841", "someFieldValue")
    val df = List(row).toDF("uuid", "eventType", "occurredOn", "someField")
    val entity = "testentity"
    val transform = Transform(entity = entity)
    val sut = df.toUpdate(Some(transform))

    inside(sut.value) {
      case UpdateContext(outputDF, "", Nil) =>
        val expectedFields = List(
          ExpectedField(name = s"${entity}_uuid", dataType = StringType),
          ExpectedField(name = s"${entity}_some_field", dataType = StringType)
        ) ++ DeltaTests.ExpectedFields.notFact(entity)
        assertExpectedFields(outputDF.schema.fields.toList, expectedFields)
    }
  }

  test("Delta.Transformations.DeltaUpdateContextTransformations.toSink") {
    import Delta.Transformations.DeltaUpdateContextTransformations

    val df = spark.emptyDataFrame
    val sinkName = "someSink"
    val entity = "testentity"
    val sut = UpdateContext(df = df, matchConditions = "", updateColumns = s"${entity}_some_update_field" :: Nil).toSink(sinkName, entity)

    sut.df mustBe df
    sut.name mustBe sinkName
    sut.input mustBe df
    sut.updateFields mustBe Map(s"${entity}_some_update_field" -> s"${Alias.Events}.${entity}_some_update_field",
                                s"${entity}_updated_time" -> s"${Alias.Events}.${entity}_created_time",
                                s"${entity}_dw_updated_time" -> s"${Alias.Events}.${entity}_dw_created_time")
  }

}
