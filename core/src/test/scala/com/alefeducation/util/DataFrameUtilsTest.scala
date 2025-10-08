package com.alefeducation.util

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

import java.util.TimeZone

class DataFrameUtilsTest extends SparkSuite with BeforeAndAfterEach {

  import com.alefeducation.bigdata.batch.BatchUtils._

  var df: DataFrame = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val session = spark
    import session.implicits._
    df = List(("col1", "col2", "col3", "col4")).toDF("col1", "col2", "col3", "col4")
  }
  // DataFrame implicit function test
  test("should remove column col1 from df") {

    val result = df.removeColumnIfPresent("col1")

    assert(!result.get.columns.contains("col1"))
    assert(result.get.columns.toSeq == Seq("col2", "col3", "col4"))
  }

  test("should remove 2 columns col1, col2 from df") {

    val result = df.removeColumnIfPresent("col1", "col2")

    assert(!result.get.columns.contains("col1"))
    assert(!result.get.columns.contains("col2"))
    assert(result.get.columns.toSeq == Seq("col3", "col4"))
  }

  test("should remove 3 columns col1, col2, col3 from df") {

    val result = df.removeColumnIfPresent("col1", "col2", "col3")

    assert(!result.get.columns.contains("col1"))
    assert(!result.get.columns.contains("col2"))
    assert(result.get.columns.toSeq == Seq("col4"))
  }

  test("should do nothing if column is not present") {

    val result = df.removeColumnIfPresent("col5")
    assert(result.get.columns.toSeq == Seq("col1", "col2", "col3", "col4"))

  }

  test("should return None if all the columns are removed") {

    val result = df.removeColumnIfPresent("col1", "col2", "col3", "col4")

    assert(result.isEmpty)

  }

}
