package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.batch.BatchWriter
import com.alefeducation.bigdata.batch.delta.DeltaCreateTests.{NestedField, Row}
import com.alefeducation.bigdata.commons.testutils.{ExpectedFields, SparkSuite}
import org.apache.spark.sql.types.StringType
import org.scalatest.Inside.inside
import org.scalatest.OptionValues.convertOptionToValuable

object DeltaCreateTests {

  case class NestedField(nestedField: String)

  case class Row(nested: NestedField, notNested: String, eventType: String, occurredOn: String, eventDateDw: String)

}

class DeltaCreateTests extends SparkSuite {

  import ExpectedFields._

  test("DeltaCreate.applyTransformations :: default transformations :: not fact") {
    val s = spark
    import s.sqlContext.implicits._

    val row = Row(
      nested = NestedField(nestedField = "someNestedFieldValue"),
      notNested = "someNotNestedFieldValue",
      eventType = "SomeCreatedEvent",
      occurredOn = "2020-05-06 15:00:00.000",
      eventDateDw = "2020-05-06 15:00:00.000"
    )
    val df = sparkContext.parallelize(List(row)).toDF
    val entity = "testentity"
    val transform = Transform(entity = entity)
    val sut = DeltaCreate.applyTransformations(df, transform)

    val expectedFields = List(
      ExpectedField(name = s"${entity}_nested_nested_field", dataType = StringType),
      ExpectedField(name = s"${entity}_not_nested", dataType = StringType)
    ) ++ DeltaTests.ExpectedFields.notFact(entity)
    assertExpectedFields(sut.schema.fields.toList, expectedFields)
  }

  test("DeltaCreate.applyTransformations :: custom transformations :: fact") {
    val s = spark
    import s.sqlContext.implicits._

    val row = Row(
      nested = NestedField(nestedField = "someNestedFieldValue"),
      notNested = "someNotNestedFieldValue",
      eventType = "SomeCreatedEvent",
      occurredOn = "2020-05-06 15:00:00.000",
      eventDateDw = "2020-05-06 15:00:00.000"
    )
    val df = sparkContext.parallelize(List(row)).toDF
    val entity = "testentity"
    val transform = Transform(entity = entity,
                              isFact = true,
                              withoutColumns = "not_nested" :: Nil,
                              mappingsOverride = Map("nested_nested_field" -> "overridden_column"))
    val sut = DeltaCreate.applyTransformations(df, transform)

    val expectedFields = List(
      ExpectedField(name = s"${entity}_overridden_column", dataType = StringType)
    ) ++ DeltaTests.ExpectedFields.fact(entity)
    assertExpectedFields(sut.schema.fields.toList, expectedFields)
  }

  test("Delta.Transformations.DeltaDataFrameTransformations.toCreate :: empty data frame") {
    import Delta.Transformations.DeltaDataFrameTransformations

    val df = spark.emptyDataFrame
    val sut = df.toCreate()

    sut mustBe empty
  }

  test("Delta.Transformations.DeltaDataFrameTransformations.toCreate :: do not transform") {
    import Delta.Transformations.DeltaDataFrameTransformations
    val s = spark
    import s.sqlContext.implicits._

    val df = List(Tuple1("someValue")).toDF("someColumn")
    val sut = df.toCreate()

    sut must contain(CreateContext(df = df, isFact = false))
  }

  test("Delta.Transformations.DeltaDataFrameTransformations.toCreate :: transform") {
    import Delta.Transformations.DeltaDataFrameTransformations
    val s = spark
    import s.sqlContext.implicits._

    val row = ("SomeCreatedEvent", "2020-05-12 07:10:03.841", "someFieldValue")
    val df = List(row).toDF("eventType", "occurredOn", "someField")
    val entity = "testentity"
    val transform = Transform(entity = entity)
    val sut = df.toCreate(Some(transform))

    inside(sut.value) {
      case CreateContext(outputDF, isFact) =>
        isFact mustBe false
        val expectedFields = List(
          ExpectedField(name = s"${entity}_some_field", dataType = StringType)
        ) ++ DeltaTests.ExpectedFields.notFact(entity)
        assertExpectedFields(outputDF.schema.fields.toList, expectedFields)
    }
  }

  test("Delta.Transformations.DeltaCreateContextTransformations.toSink :: fact") {
    import Delta.Transformations.DeltaCreateContextTransformations

    val df = spark.emptyDataFrame
    val writerOptions = Map("someWriterOptionName" -> "someWriterOptionValue")
    val sinkName = "someSink"
    val sut = CreateContext(df = df, isFact = true).toSink(sinkName, writerOptions)

    sut.df mustBe df
    sut.name mustBe sinkName
    sut.input mustBe df
    sut.writerOptions mustBe writerOptions ++ Map(BatchWriter.Options.PartitionBy -> Columns.EventDate)
  }

  test("Delta.Transformations.DeltaCreateContextTransformations.toSink :: not fact") {
    import Delta.Transformations.DeltaCreateContextTransformations

    val df = spark.emptyDataFrame
    val writerOptions = Map("someWriterOptionName" -> "someWriterOptionValue")
    val sinkName = "someSink"
    val sut = CreateContext(df = df, isFact = false).toSink(sinkName, writerOptions)

    sut.df mustBe df
    sut.name mustBe sinkName
    sut.input mustBe df
    sut.writerOptions mustBe writerOptions
  }

}
