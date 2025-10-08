package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import org.apache.spark.sql.DataFrame
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

trait BaseDimensionSpec extends Matchers {

  protected val commonParquetColumns = List("eventDateDw", "eventType", "loadtime", "eventdate")

  protected def commonRedshiftCreatedColumns(entity: String) = List(s"${entity}_created_time", s"${entity}_dw_created_time")

  protected def commonRedshiftColumns(entity: String) =
    List(s"${entity}_created_time",
         s"${entity}_dw_created_time",
         s"${entity}_updated_time",
         s"${entity}_deleted_time",
         s"${entity}_dw_updated_time")

  def testDFColumns(df: DataFrame, expectedCols: Seq[String], expectedDfSize: Int): Assertion = {
    val dfCols = df.columns.toSet

    expectedCols.toSet.diff(dfCols).size shouldBe 0
    dfCols.diff(expectedCols.toSet).size shouldBe 0

    df.count shouldBe expectedDfSize
  }

  def testSinkBySinkName(sinks: Seq[Sink], sinkName: String, expectedCols: Seq[String], expectedDfSize: Int): Assertion = {
    val filteredSinks = sinks.filter(_.name == sinkName)
    filteredSinks.length shouldBe 1

    val df: DataFrame = filteredSinks.head.output
    testDFColumns(df, expectedCols, expectedDfSize)
  }

  def testSinkByEventType(sinks: Seq[Sink], sinkName: String, eventType: String, expectedCols: Seq[String], expectedDfSize: Int): Assertion = {
    val filteredSinks = sinks.filter(s => s.name == sinkName && s.eventType == eventType)
    filteredSinks.length shouldBe 1

    val df: DataFrame = filteredSinks.head.output
    testDFColumns(df, expectedCols, expectedDfSize)
  }
}
