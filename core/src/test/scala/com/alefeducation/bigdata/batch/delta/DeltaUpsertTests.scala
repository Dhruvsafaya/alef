package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.{Inside, OptionValues}

class DeltaUpsertTests extends SparkSuite with Matchers with Inside with OptionValues {

  test("Delta.Transformations.DeltaDataFrameTransformations.toUpsert :: empty data frame") {
    import Delta.Transformations.DeltaDataFrameTransformations

    val df = spark.emptyDataFrame
    val sut = df.toUpsert(matchConditions = "", partitionBy = Nil, columnsToUpdate = Map(), columnsToInsert = Map())

    sut mustBe empty
  }

  test("Delta.Transformations.DeltaDataFrameTransformations.toUpsert :: do not transform") {
    import Delta.Transformations.DeltaDataFrameTransformations
    val s = spark
    import s.sqlContext.implicits._

    val df = List(Tuple1("someValue")).toDF("someColumn")

    val expectedMatchConditions = "a = b"
    val expectedColumnsToUpdate = Map("a" -> "b")
    val expectedColumnsToInsert = Map("a" -> "a")

    val sut = df.toUpsert(
      partitionBy = Nil,
      matchConditions = expectedMatchConditions,
      columnsToUpdate = expectedColumnsToUpdate,
      columnsToInsert = expectedColumnsToInsert
    )

    val expectedUpsertContext = UpsertContext(
      df = df,
      partitionBy = Nil,
      matchConditions = expectedMatchConditions,
      columnsToUpdate = expectedColumnsToUpdate,
      columnsToInsert = expectedColumnsToInsert,
      updateConditions = None
    )
    sut must contain(expectedUpsertContext)
  }

  test("Delta.Transformations.DeltaUpsertContextTransformations.toSink") {
    import Delta.Transformations.DeltaUpsertContextTransformations

    val df = spark.emptyDataFrame

    val expectedPartitionBy = Seq("a")
    val expectedMatchConditions = "a = b"
    val expectedColumnsToUpdate = Map("a" -> "b")
    val expectedColumnsToInsert = Map("a" -> "a")
    val expectedUpdateConditions = Some("a > 1")

    val sinkName = "someSink"

    val sut = UpsertContext(
      df = df,
      partitionBy = expectedPartitionBy,
      matchConditions = expectedMatchConditions,
      columnsToUpdate = expectedColumnsToUpdate,
      columnsToInsert = expectedColumnsToInsert,
      updateConditions = expectedUpdateConditions
    ).toSink(sinkName)

    sut.df mustBe df
    sut.name mustBe sinkName
    sut.input mustBe df
    sut.partitionBy mustBe expectedPartitionBy
    sut.matchConditions mustBe expectedMatchConditions
    sut.columnsToUpdate mustBe expectedColumnsToUpdate
    sut.columnsToInsert mustBe expectedColumnsToInsert
    sut.updateConditions mustBe expectedUpdateConditions
  }

}
