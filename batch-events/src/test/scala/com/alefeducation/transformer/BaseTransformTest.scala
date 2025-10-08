package com.alefeducation.transformer

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.io.UnityCatalog
import com.alefeducation.util.{BatchExecutionLogger, Offset, OffsetLogger, TestSparkSession}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, times, verify, when}
import org.scalatestplus.mockito.MockitoSugar.mock
import org.slf4j.Logger

class BaseTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val mockLogger: Logger = mock[Logger]

  test("should execute for multiple sources") {
    val mockBatchExecutionLogger: BatchExecutionLogger = mock[BatchExecutionLogger]
    val mockService: SparkBatchService = mock[SparkBatchService]
    val mockUnityCatalog: UnityCatalog = mock[UnityCatalog]
    val mockOffsetLogger: OffsetLogger = mock[OffsetLogger]

    val session = TestSparkSession.getSparkSession

    import session.implicits._
    val data = session.sparkContext.parallelize(Seq(("elem1", 1732598149609L))).toDF("name", "_load_time")
    when(mockBatchExecutionLogger.getPreviousOffset("test_batch_job_id", "bronze_table_name")).thenReturn(Offset(0, 10))
    when(mockBatchExecutionLogger.getOffsets(any())).thenReturn(Some(Offset(11, 15)))
    when(mockUnityCatalog.readOptional("/test/catalog/bronze/data/bronze_table_name")).thenReturn(Some(data))
    doNothing().when(mockOffsetLogger).log(any(), any(), any())


    when(mockService.getStartIdUpdateStatus(ArgumentMatchers.eq("test_batch_job_id"))).thenReturn(1000L)

    val baseTransform = new BaseTestTransform("batch-test-config") {
      override protected val spark: SparkSession = sprk
      override protected val unity: UnityCatalog = mockUnityCatalog
      override protected val batchExecutionLogger: BatchExecutionLogger = mockBatchExecutionLogger
      override protected val service: SparkBatchService = mockService
      override protected val offsetLogger: OffsetLogger = mockOffsetLogger
    }

    baseTransform.run

    verify(mockBatchExecutionLogger, times(1)).getPreviousOffset(ArgumentMatchers.eq("test_batch_job_id"), any())
    verify(mockService).getStartIdUpdateStatus(ArgumentMatchers.eq("test_batch_job_id"))
    verify(mockOffsetLogger).log(
      ArgumentMatchers.eq("""{"bronze_table_name":15}"""),
      ArgumentMatchers.eq("/test/catalog/_internal/offset_log/"),
      ArgumentMatchers.eq("dim_test_table")
    )
  }

}

class BaseTestTransform(serviceName: String) extends BaseTransform(serviceName: String) {
  def transform(data: Map[String, Option[DataFrame]], startId: Long): Option[DataFrame] = data.values.headOption.flatten
}
