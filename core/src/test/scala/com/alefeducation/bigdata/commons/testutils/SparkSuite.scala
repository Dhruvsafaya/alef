package com.alefeducation.bigdata.commons.testutils

import com.alefeducation.bigdata.Sink
import com.alefeducation.service.Service
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.Suite

import java.sql.Timestamp
import java.util.TimeZone
import scala.collection.mutable

trait PlatformSharedSparkSession extends SharedSparkSession {
  this: Suite =>

  override protected def sparkConf: SparkConf = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    System.setProperty("user.timezone", "UTC")


    val conf = super.sparkConf
    conf
      .set("spark.sql.session.timeZone", "UTC")
      .set("_enableConnection", "false")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.default.parallelism", "1")
      .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .set("spark.sql.datetime.java8API.enabled", "false")
      .set("spark.sql.legacy.typeCoercion.datetimeToString.enabled", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.bindAddress", "127.0.0.1")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.cbo.enabled", "false")
      .setMaster("local[*]")
      .set("spark.driver.memory", "1g")
  }


}

trait SparkSuite extends SparkFunSuite with PlatformSharedSparkSession {

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  System.setProperty("user.timezone", "UTC")

  case class SparkFixture(key: String, value: String, schema: Option[StructType] = None)

  def withSparkFixtures(fixtures: List[SparkFixture], testFunc: List[Sink] => Unit)(implicit transformer: Service): Unit = {
    fixtures foreach setSparkFixture
    try {
      testFunc(transform(transformer))
    } finally {
      fixtures foreach unsetSparkFixture
    }
  }

  def withSparkFixturesWithSink(fixtures: List[SparkFixture], sink: String, testFunc: DataFrame => Unit)(
    implicit transformer: Service): Unit = {
    fixtures foreach setSparkFixture
    try {
      testFunc(transform(transformer, sink))
    } finally {
      fixtures foreach unsetSparkFixture
    }
  }

  def transform[T <: Service](t: T, sink: String): DataFrame = {
    val sinks = t.transform()
    sinks.foreach { sink =>
      sink.output.cache()
    }
    sinks.find(_.name == sink).map(_.output).head
  }

  def transform[T <: Service](t: T): List[Sink] = {
    val sinks = t.transform()
    sinks.foreach { sink =>
      sink.output.cache()
    }
    sinks
  }

  val cache: mutable.Map[DataFrame, Row] = mutable.Map()

  /** TODO:: Failure message is not clear when underlying data type mismatch happens
    * If the underlying datatype is string fol `col`,
    * `dataFrame.first.getAs[T](col)` returns value as string even if we expect Int in `value`
    * In that case though the test fails, the failure message is not clear
    *
    * Invoking assertions per column in a DataFrame can be expensive,
    * because each assertion typically calls `first()`, which triggers the pipeline execution.
    * This means the entire upstream computation may be re-executed for every assertion.
   */
  @deprecated("use assertRow or assertTimestampRow")
  def assert[T](dataFrame: DataFrame, col: String, value: T): Unit = {
    val row: Row = getCachedRow(dataFrame)
    row.getAs[T](col) match {
      case v if v.isInstanceOf[Timestamp] =>
        if (!v.toString.equals(value)) throw new Exception(s"Value mismatch, Given: $value Found: $v")
      case v if v != value => throw new Exception(s"Value mismatch, Given: $value Found: $v")
      case _ =>
    }
  }

  private def getCachedRow[T](dataFrame: DataFrame) = {
    val row = if (cache.contains(dataFrame)) {
      cache(dataFrame)
    } else {
      val row = dataFrame.first()
      cache.put(dataFrame, row)
      row
    }
    row
  }

  def assertRow[T](row: Row, col: String, value: T): Unit = {
    row.getAs[T](col) match {
      case v if v != value => throw new Exception(s"Value mismatch, Given: $value Found: $v")
      case _ =>
    }
  }

  def assertTimestampRow(row: Row, col: String, expected: String): Unit = {
    val ts = row.getAs[Timestamp](col)
    if (expected != null) {
      assert(Timestamp.valueOf(expected).equals(ts), s"Value mismatch, Given: $expected Found: $ts")
    } else {
      assert(ts == null, s"Value mismatch, Given: $expected Found: $ts")
    }
  }

  def assertTimestamp(dataFrame: DataFrame, col: String, expected: String): Unit = {
    val ts = getCachedRow(dataFrame).getAs[Timestamp](col)
    if (expected != null) {
      assert(Timestamp.valueOf(expected).equals(ts), s"Value mismatch, Given: $expected Found: $ts")
    } else {
      assert(ts == null, s"Value mismatch, Given: $expected Found: $ts")
    }
  }

  def assertSet[T](dataFrame: DataFrame, name: String, value: Set[T]): Unit = {
    val recipientIds = dataFrame.collect().map(_.getAs[T](name)).toSet
    assert(recipientIds === value)
  }

  def dimDateCols(entity: String): List[String] =
    List(
      "_created_time",
      "_updated_time",
      "_deleted_time",
      "_dw_created_time",
      "_dw_updated_time",
      "_active_until",
      "_status"
    ).map(entity + _)

  private def setSparkFixture(fixture: SparkFixture): Unit = {
    spark.conf.set(fixture.key, fixture.value)
    fixture.schema.foreach{ schema =>
      spark.conf.set(s"${fixture.key}Schema", schema.toDDL)
    }
  }

  private def unsetSparkFixture(fixture: SparkFixture): Unit = {
    spark.conf.unset(fixture.key)
    fixture.schema.foreach { _ =>
      spark.conf.unset(s"${fixture.key}Schema")
    }
  }

}
