package com.alefeducation.bigdata.batch.delta

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.{Inside, OptionValues}

class DeltaWriteTests extends SparkSuite with Matchers with Inside with OptionValues {

  test("Delta.Transformations.DeltaDataFrameTransformations.toWrite :: empty data frame") {
    import Delta.Transformations.DeltaDataFrameTransformations

    val df = spark.emptyDataFrame
    val sut = df.toWrite

    sut mustBe empty
  }

  test("Delta.Transformations.DeltaDataFrameTransformations.toWrite :: not empty data frame") {
    import Delta.Transformations.DeltaDataFrameTransformations
    val s = spark
    import s.sqlContext.implicits._

    val df = List(Tuple1("someValue")).toDF("someColumn")
    val sut = df.toWrite

    sut must contain(WriteContext(df))
  }

  test("Delta.Transformations.DeltaWriteContextTransformations.toSink") {
    import Delta.Transformations.DeltaWriteContextTransformations

    val df = spark.emptyDataFrame
    val writerOptions = Map("someWriterOptionName" -> "someWriterOptionValue")
    val sinkName = "someSink"
    val sut = WriteContext(df).toSink(sinkName, writerOptions)

    sut.df mustBe df
    sut.name mustBe sinkName
    sut.input mustBe df
    sut.writerOptions mustBe writerOptions
  }

}
