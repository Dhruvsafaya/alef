package com.alefeducation.bigdata.batch

import com.alefeducation.bigdata.commons.testutils.PlatformSharedSparkSession
import com.alefeducation.util.Resources.Resource
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SparkSession}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach

import java.sql.SQLException

class BatchSinkTest extends SparkFunSuite with PlatformSharedSparkSession with BeforeAndAfterEach {

  val expDf = mock(classOf[DataFrame])
  val mockWriter = mock(classOf[DataFrameWriter[Row]])
  val sprk = spark

  val batchSink: DataSink = new DataSink {
      override implicit def spark: SparkSession = sprk

      override def df: DataFrame = expDf

      override def name: String = "parquet-data_sink_test"

      override def input: DataFrame = expDf
    }

  val resource = Resource("resource", Map())

  override def beforeEach(): Unit = {
    when(expDf.coalesce(2)).thenReturn(expDf)
    when(expDf.write).thenReturn(mockWriter)
  }

  override def afterEach(): Unit = {
    reset(mockWriter)
    reset(expDf)
  }

  test("batchSink writer should save data to redshift") {
    when(mockWriter.save()).thenAnswer(_ => throw new SQLException("exc", "error ", 1023)).thenAnswer(_ => ())

    batchSink.write(resource)

    verify(mockWriter, times(2)).save()
  }
}
