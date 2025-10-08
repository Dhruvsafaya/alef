package com.alefeducation.base

import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.io.data.DeltaIO
import com.alefeducation.schema.internal.ControlTableUtils.{Completed, Started}
import com.alefeducation.util.DateTimeProvider
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{reset, times, verify, when}
import org.scalatestplus.mockito.MockitoSugar.mock
import org.mockito.ArgumentMatchers.{eq => eqs}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Timestamp

class DataOffsetServiceSpec extends SparkSuite {

  val mockDeltaTable: DeltaTable = mock[DeltaTable]
  val mockDateTimeProvider: DateTimeProvider = mock[DateTimeProvider]

  val defaultCurrentTime: Timestamp = Timestamp.valueOf("2023-10-05 16:37:18.291")

  val statusCondition = s"${Alias.Delta}.status = 'completed'"

  val dataOffsetValue: String =
    """
      |[
      |{
      |   "table_name": "fact_user_heartbeat_hourly_aggregated",
      |   "last_dw_created_time": "2023-10-01 10:00:00.000",
      |   "status": "completed",
      |   "updated_time": "2023-10-08 07:28:16.285"
      |}
      |]
      |""".stripMargin

  override def beforeEach(): Unit = {
    val sprk: SparkSession = spark
    import sprk.implicits._
    val inputDF = spark.read
      .json(Seq(dataOffsetValue).toDS())
      .withColumn("last_dw_created_time", col("last_dw_created_time").cast(TimestampType))
    when(mockDeltaTable.toDF).thenReturn(inputDF)

    when(mockDateTimeProvider.currentUtcTimestamp()).thenReturn(lit(defaultCurrentTime))
    when(mockDateTimeProvider.currentUtcJavaTimestamp()).thenReturn(defaultCurrentTime)
  }

  override def afterEach(): Unit = {
    reset(mockDeltaTable)
    reset(mockDateTimeProvider)
  }

  test("getOffset should update existing record and return its offset if table present") {
    val mockDeltaIO: DeltaIO = mock[DeltaIO]

    val dataOffsetService = new DataOffsetService()
    dataOffsetService.init(mockDeltaTable, mockDateTimeProvider, mockDeltaIO)

    val expectedTableName = "fact_user_heartbeat_hourly_aggregated"

    val actualOffset: Timestamp = dataOffsetService.getOffset(expectedTableName)

    actualOffset shouldBe Timestamp.valueOf("2023-10-01 10:00:00.000")

    val expectedUpsertColumns = Map(
      "table_name" -> s"${Alias.Events}.table_name",
      "status" -> s"${Alias.Events}.status",
      "updated_time" -> s"${Alias.Events}.updated_time",
    )
    val expectedMatchConditions = s"${Alias.Delta}.table_name = ${Alias.Events}.table_name AND ${Alias.Delta}.table_name = '$expectedTableName'"

    val dataframeCaptor = ArgumentCaptor.forClass(classOf[DataFrame])

    verify(mockDeltaIO, times(1)).upsert(
      deltaTable = eqs(mockDeltaTable),
      dataFrame = dataframeCaptor.capture(),
      matchConditions = eqs(expectedMatchConditions),
      columnsToUpdate = eqs(expectedUpsertColumns),
      columnsToInsert = eqs(expectedUpsertColumns),
      updateConditions = eqs(null),
    )

    val actualDf = dataframeCaptor.getValue
    assert[String](actualDf, "table_name", expectedTableName)
    assert[String](actualDf, "last_dw_created_time", null)
    assert[String](actualDf, "status", Started.toString)
    assert[String](actualDf, "updated_time", "2023-10-05 16:37:18.291")
  }

  test("getOffset should insert new record and return defaultTimestamp if table is not present") {
    val mockDeltaIO: DeltaIO = mock[DeltaIO]

    val dataOffsetService = new DataOffsetService()
    dataOffsetService.init(mockDeltaTable, mockDateTimeProvider, mockDeltaIO)

    val expectedTableName = "any_table_name"

    val actualOffset: Timestamp = dataOffsetService.getOffset(expectedTableName)

    actualOffset shouldBe Timestamp.valueOf("1970-01-01 00:00:00")

    val expectedUpsertColumns = Map(
      "table_name" -> s"${Alias.Events}.table_name",
      "last_dw_created_time" -> s"${Alias.Events}.last_dw_created_time",
      "status" -> s"${Alias.Events}.status",
      "updated_time" -> s"${Alias.Events}.updated_time",
    )
    val expectedMatchConditions = s"${Alias.Delta}.table_name = ${Alias.Events}.table_name AND ${Alias.Delta}.table_name = '$expectedTableName'"

    val dataframeCaptor = ArgumentCaptor.forClass(classOf[DataFrame])

    verify(mockDeltaIO, times(1)).upsert(
      deltaTable = eqs(mockDeltaTable),
      dataFrame = dataframeCaptor.capture(),
      matchConditions = eqs(expectedMatchConditions),
      columnsToUpdate = eqs(expectedUpsertColumns),
      columnsToInsert = eqs(expectedUpsertColumns),
      updateConditions = eqs(null),
    )

    val actualDf = dataframeCaptor.getValue
    assert[String](actualDf, "table_name", expectedTableName)
    assert[String](actualDf, "last_dw_created_time", "1970-01-01 00:00:00.0")
    assert[String](actualDf, "status", Started.toString)
    assert[String](actualDf, "updated_time", "2023-10-05 16:37:18.291")
  }

  test("complete should update data_offset table with status on empty dataframe") {
    val mockDeltaIO: DeltaIO = mock[DeltaIO]

    val dataOffsetService = new DataOffsetService()
    dataOffsetService.init(mockDeltaTable, mockDateTimeProvider, mockDeltaIO)

    val expectedTableName = "any_table_name"

    dataOffsetService.complete(expectedTableName, spark.emptyDataFrame, "dw_created_time")

    val expectedUpsertColumns = Map(
      "table_name" -> s"${Alias.Events}.table_name",
      "status" -> s"${Alias.Events}.status",
      "updated_time" -> s"${Alias.Events}.updated_time",
    )
    val expectedMatchConditions = s"${Alias.Delta}.table_name = ${Alias.Events}.table_name AND ${Alias.Delta}.table_name = '$expectedTableName'"

    val dataframeCaptor = ArgumentCaptor.forClass(classOf[DataFrame])

    verify(mockDeltaIO, times(1)).upsert(
      deltaTable = eqs(mockDeltaTable),
      dataFrame = dataframeCaptor.capture(),
      matchConditions = eqs(expectedMatchConditions),
      columnsToUpdate = eqs(expectedUpsertColumns),
      columnsToInsert = eqs(expectedUpsertColumns),
      updateConditions = eqs(null),
    )

    val actualDf = dataframeCaptor.getValue
    assert[String](actualDf, "table_name", expectedTableName)
    assert[String](actualDf, "last_dw_created_time", null)
    assert[String](actualDf, "status", Completed.toString)
    assert[String](actualDf, "updated_time", "2023-10-05 16:37:18.291")
  }

  test("complete should update data_offset table with max dw_created_time") {
    val mockDeltaIO: DeltaIO = mock[DeltaIO]

    val columns = Seq("dw_created_time", "value")
    val data = Seq(
      (Timestamp.valueOf("2023-01-01 00:00:00"), "1"),
      (Timestamp.valueOf("2023-01-03 00:00:00"), "3"),
      (Timestamp.valueOf("2023-01-02 00:00:00"), "2")
    )
    val rdd = spark.sparkContext.parallelize(data)
    val output: DataFrame = spark.createDataFrame(rdd).toDF(columns: _*)

    val dataOffsetService = new DataOffsetService()
    dataOffsetService.init(mockDeltaTable, mockDateTimeProvider, mockDeltaIO)

    val expectedTableName = "any_table_name"

    dataOffsetService.complete(expectedTableName, output, "dw_created_time")

    val expectedUpsertColumns = Map(
      "table_name" -> s"${Alias.Events}.table_name",
      "last_dw_created_time" -> s"${Alias.Events}.last_dw_created_time",
      "status" -> s"${Alias.Events}.status",
      "updated_time" -> s"${Alias.Events}.updated_time",
    )
    val expectedMatchConditions = s"${Alias.Delta}.table_name = ${Alias.Events}.table_name AND ${Alias.Delta}.table_name = '$expectedTableName'"

    val dataframeCaptor = ArgumentCaptor.forClass(classOf[DataFrame])

    verify(mockDeltaIO, times(1)).upsert(
      deltaTable = eqs(mockDeltaTable),
      dataFrame = dataframeCaptor.capture(),
      matchConditions = eqs(expectedMatchConditions),
      columnsToUpdate = eqs(expectedUpsertColumns),
      columnsToInsert = eqs(expectedUpsertColumns),
      updateConditions = eqs(null),
    )

    val actualDf = dataframeCaptor.getValue
    assert[String](actualDf, "table_name", expectedTableName)
    assert[String](actualDf, "last_dw_created_time", "2023-01-03 00:00:00.0")
    assert[String](actualDf, "status", Completed.toString)
    assert[String](actualDf, "updated_time", "2023-10-05 16:37:18.291")
  }

  test("upsertDataOffset should perform upsert if offset is defined") {
    val mockDeltaIO: DeltaIO = mock[DeltaIO]

    val dataOffsetService = new DataOffsetService()
    dataOffsetService.init(mockDeltaTable, mockDateTimeProvider, mockDeltaIO)

    val expectedTableName = "any_table_name"
    val expectedOffset = Timestamp.valueOf("2023-10-01 10:25:17.290")
    val expectedStatus = Completed

    dataOffsetService.upsertDataOffset(expectedTableName, expectedStatus, Some(expectedOffset))

    val expectedUpsertColumns = Map(
      "table_name" -> s"${Alias.Events}.table_name",
      "last_dw_created_time" -> s"${Alias.Events}.last_dw_created_time",
      "status" -> s"${Alias.Events}.status",
      "updated_time" -> s"${Alias.Events}.updated_time",
    )
    val expectedMatchConditions = s"${Alias.Delta}.table_name = ${Alias.Events}.table_name AND ${Alias.Delta}.table_name = '$expectedTableName'"

    val dataframeCaptor = ArgumentCaptor.forClass(classOf[DataFrame])

    verify(mockDeltaIO, times(1)).upsert(
      deltaTable = eqs(mockDeltaTable),
      dataFrame = dataframeCaptor.capture(),
      matchConditions = eqs(expectedMatchConditions),
      columnsToUpdate = eqs(expectedUpsertColumns),
      columnsToInsert = eqs(expectedUpsertColumns),
      updateConditions = eqs(null),
    )

    val actualDf = dataframeCaptor.getValue
    assert[String](actualDf, "table_name", expectedTableName)
    assert[String](actualDf, "last_dw_created_time", "2023-10-01 10:25:17.29")
    assert[String](actualDf, "status", Completed.toString)
    assert[String](actualDf, "updated_time", "2023-10-05 16:37:18.291")
  }

  test("upsertDataOffset should perform upsert if offset is not defined") {
    val mockDeltaIO: DeltaIO = mock[DeltaIO]

    val dataOffsetService = new DataOffsetService()
    dataOffsetService.init(mockDeltaTable, mockDateTimeProvider, mockDeltaIO)

    val expectedTableName = "any_table_name"
    val expectedStatus = Completed

    dataOffsetService.upsertDataOffset(expectedTableName, expectedStatus, None)

    val expectedUpsertColumns = Map(
      "table_name" -> s"${Alias.Events}.table_name",
      "status" -> s"${Alias.Events}.status",
      "updated_time" -> s"${Alias.Events}.updated_time",
    )
    val expectedMatchConditions = s"${Alias.Delta}.table_name = ${Alias.Events}.table_name AND ${Alias.Delta}.table_name = '$expectedTableName'"

    val dataframeCaptor = ArgumentCaptor.forClass(classOf[DataFrame])

    verify(mockDeltaIO, times(1)).upsert(
      deltaTable = eqs(mockDeltaTable),
      dataFrame = dataframeCaptor.capture(),
      matchConditions = eqs(expectedMatchConditions),
      columnsToUpdate = eqs(expectedUpsertColumns),
      columnsToInsert = eqs(expectedUpsertColumns),
      updateConditions = eqs(null),
    )

    val actualDf = dataframeCaptor.getValue
    assert[String](actualDf, "table_name", expectedTableName)
    assert[String](actualDf, "last_dw_created_time", null)
    assert[String](actualDf, "status", Completed.toString)
    assert[String](actualDf, "updated_time", "2023-10-05 16:37:18.291")
  }

  test("createDataOffsetRecord should return dataframe") {
    val mockDeltaIO: DeltaIO = mock[DeltaIO]

    val expectedOffset = Timestamp.valueOf("2023-10-01 10:25:17.290")
    val expectedStatus = Completed

    val dataOffsetService = new DataOffsetService()
    dataOffsetService.init(mockDeltaTable, mockDateTimeProvider, mockDeltaIO)

    val actual = dataOffsetService.createDataOffsetRecord("table_name", expectedStatus, Some(expectedOffset))

    val row = actual.collect()(0)
    val offset = row.getAs[Timestamp]("last_dw_created_time")
    val status = row.getAs[String]("status")
    (offset, status) shouldBe (expectedOffset, expectedStatus.toString)
  }
}
