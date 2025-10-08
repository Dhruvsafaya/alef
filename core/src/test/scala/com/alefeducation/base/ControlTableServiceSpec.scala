package com.alefeducation.base

import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.io.data.DeltaIO
import com.alefeducation.schema.internal.ControlTableUtils.{Processing, Started}
import com.alefeducation.util.DateTimeProvider
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{eq => eqs}
import org.mockito.Mockito.{reset, times, verify, when}
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Timestamp

class ControlTableServiceSpec extends SparkSuite {
  val mockDeltaTable: DeltaTable = mock[DeltaTable]
  val mockDateTimeProvider: DateTimeProvider = mock[DateTimeProvider]

  val statusCondition = s"${Alias.Delta}.status = 'completed'"

  val productMaxIdsValue: String =
    """
      |{
      |   "table_name": "fact_user_avatar",
      |   "max_id": 1042,
      |   "status": "completed",
      |   "updated_time": "2023-09-08 07:28:16.285"
      |}
      |""".stripMargin

  override def beforeEach(): Unit = {
    val sprk: SparkSession = spark
    import sprk.implicits._
    val inputDF = spark.read.json(Seq(productMaxIdsValue).toDS())
    when(mockDeltaTable.toDF).thenReturn(inputDF)
    when(mockDateTimeProvider.currentUtcTimestamp()).thenReturn(lit(Timestamp.valueOf("2023-10-05 16:37:18.291")))
  }

  override def afterEach(): Unit = {
    reset(mockDeltaTable)
    reset(mockDateTimeProvider)
  }

  test("getStartIdUpdateStatus should return max id when table is present in table product_max_ids") {
    val mockDeltaIO: DeltaIO = mock[DeltaIO]

    val controlTableService = new ControlTableService()
    controlTableService.init(mockDeltaTable, mockDateTimeProvider, mockDeltaIO)

    val tableName = "fact_user_avatar"
    val actualId = controlTableService.getStartIdUpdateStatus(tableName)

    val expectedUpsertColumns = Map(
      "table_name" -> s"${Alias.Events}.table_name",
      "max_id" -> s"${Alias.Events}.max_id",
      "status" -> s"${Alias.Events}.status",
      "updated_time" -> s"${Alias.Events}.updated_time",
    )
    val expectedMatchConditions = s"${Alias.Delta}.table_name = ${Alias.Events}.table_name AND ${Alias.Delta}.table_name = '$tableName'"

    val dataframeCaptor = ArgumentCaptor.forClass(classOf[DataFrame])

    verify(mockDeltaIO, times(1)).upsert(
      deltaTable = eqs(mockDeltaTable),
      dataFrame = dataframeCaptor.capture(),
      matchConditions = eqs(expectedMatchConditions),
      columnsToUpdate = eqs(expectedUpsertColumns),
      columnsToInsert = eqs(expectedUpsertColumns),
      updateConditions = eqs(Some(statusCondition)),
    )

    val actualDf = dataframeCaptor.getValue
    val actualRow = actualDf.first()
    assertRow[String](actualRow, "table_name", tableName)
    assertRow[Int](actualRow, "max_id", 1042)
    assertRow[String](actualRow, "status", Started.toString)
    assertTimestampRow(actualRow, "updated_time", "2023-10-05 16:37:18.291")

    actualId shouldBe 1043
  }

  test("getStartIdUpdateStatus should return max id when table is NOT present in table product_max_ids") {
    val mockDeltaIO: DeltaIO = mock[DeltaIO]

    val controlTableService = new ControlTableService()
    controlTableService.init(mockDeltaTable, mockDateTimeProvider, mockDeltaIO)

    val tableName = "any_table_name"
    val actual = controlTableService.getStartIdUpdateStatus(tableName)

    val expectedUpsertColumns: Map[String, String] = Map(
      "table_name" -> s"${Alias.Events}.table_name",
      "max_id" -> s"${Alias.Events}.max_id",
      "status" -> s"${Alias.Events}.status",
      "updated_time" -> s"${Alias.Events}.updated_time",
    )
    val expectedMatchConditions = s"${Alias.Delta}.table_name = ${Alias.Events}.table_name AND ${Alias.Delta}.table_name = '$tableName'"

    val dataframeCaptor = ArgumentCaptor.forClass(classOf[DataFrame])

    verify(mockDeltaIO, times(1)).upsert(
      deltaTable = eqs(mockDeltaTable),
      dataFrame = dataframeCaptor.capture(),
      matchConditions = eqs(expectedMatchConditions),
      columnsToUpdate = eqs(expectedUpsertColumns),
      columnsToInsert = eqs(expectedUpsertColumns),
      updateConditions = eqs(Some(statusCondition)),
    )

    val actualDf = dataframeCaptor.getValue
    val actualRow = actualDf.first()
    assertRow[String](actualRow, "table_name", tableName)
    assertRow[Int](actualRow, "max_id", 0)
    assertRow[String](actualRow, "status", Started.toString)
    assertTimestampRow(actualRow, "updated_time", "2023-10-05 16:37:18.291")

    actual shouldBe 1
  }

  test("getStartIdUpdateStatus throw exception if inconsistent state") {
    val mockDeltaIO: DeltaIO = mock[DeltaIO]

    val controlTableService = new ControlTableService()
    val mockDeltaTable: DeltaTable = mock[DeltaTable]
    val sprk: SparkSession = spark
    import sprk.implicits._
    val inputDF = spark.read.json(Seq("""
                                        |{
                                        |   "table_name": "fact_user_avatar",
                                        |   "max_id": 1042,
                                        |   "status": "processing",
                                        |   "updated_time": "2023-09-08 07:28:16.285"
                                        |}
                                        |""".stripMargin).toDS())
    when(mockDeltaTable.toDF).thenReturn(inputDF)
    controlTableService.init(mockDeltaTable, mockDateTimeProvider, mockDeltaIO)

    val tableName = "fact_user_avatar"
    val exception = intercept[RuntimeException] {
      controlTableService.getStartIdUpdateStatus(tableName, failIfWrongState = true)
    }
    assert(exception.getMessage === "table_name = fact_user_avatar, status = processing is inconsistent, check product-max-id")
  }

  test("complete should update product_max_ids table") {
    val mockDeltaIO: DeltaIO = mock[DeltaIO]

    val controlTableService = new ControlTableService()
    controlTableService.init(mockDeltaTable, mockDateTimeProvider, mockDeltaIO)

    controlTableService.complete("fact_user_avatar", 5000)

    val expectedMatchConditions: String = "table_name = 'fact_user_avatar'"
    val expectedColumnsToUpdate: Map[String, String] = Map(
      "max_id" -> "max_id + 5000",
      "status" -> "'completed'",
      "updated_time" -> "current_timestamp"
    )

    verify(mockDeltaIO, times(1)).update(
      deltaTable = eqs(mockDeltaTable),
      matchConditions = eqs(expectedMatchConditions),
      columnsToUpdate = eqs(expectedColumnsToUpdate)
    )
  }

  test("updateStatus should update status to Processing state") {
    val mockDeltaIO: DeltaIO = mock[DeltaIO]

    val controlTableService = new ControlTableService()
    controlTableService.init(mockDeltaTable, mockDateTimeProvider, mockDeltaIO)

    controlTableService.updateStatus("fact_user_avatar", Processing)

    val expectedMatchConditions: String = "table_name = 'fact_user_avatar'"
    val expectedColumnsToUpdate: Map[String, String] = Map(
      "status" -> "'processing'",
      "updated_time" -> "current_timestamp"
    )

    verify(mockDeltaIO, times(1)).update(
      deltaTable = eqs(mockDeltaTable),
      matchConditions = eqs(expectedMatchConditions),
      columnsToUpdate = eqs(expectedColumnsToUpdate)
    )
  }
}
