package com.alefeducation.dimensions.curriculum

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.dimensions.curriculum.dim_curriculum.CurriculumDimTransform
import com.alefeducation.dimensions.curriculum.dim_curriculum.CurriculumDimTransform.CurriculumDimSourceName
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class CurriculumDimensionSpec extends SparkSuite with BaseDimensionSpec {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val commonColsRS = Seq(
    "curr_created_time",
    "curr_dw_created_time",
    "curr_updated_time",
    "curr_deleted_time",
    "curr_dw_updated_time"
  )

  val expectedCreatedColsRS = (CurriculumCreatedDimCols.values.toSeq ++ commonColsRS).diff(Seq("occurredOn"))

  test("Test CurriculumCreatedEvent") {

    val incomingCurriculumCreatedEvent =
      """
        |[
        |{"eventType":"CurriculumCreatedEvent","loadtime":"2020-05-11T09:55:20.463Z","curriculumId":5124,"name":"New Curriculum ","organisation":"new org","occurredOn":"2020-05-11 09:55:20.440","eventDateDw":"20200511"},
        |{"eventType":"CurriculumCreatedEvent","loadtime":"2020-05-11T10:01:44.391Z","curriculumId":42321,"name":"sd2d","organisation":"shared","occurredOn":"2020-05-11 10:01:44.387","eventDateDw":"20200511"},
        |{"eventType":"CurriculumCreatedEvent","loadtime":"2020-05-11T12:03:15.921Z","curriculumId":233,"name":"asdads1","organisation":"MORA","occurredOn":"2020-05-11 12:03:15.917","eventDateDw":"20200511"}
        |]
        |
      """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val incomingCurriculumCreatedEventDF = spark.read.json(Seq(incomingCurriculumCreatedEvent).toDS())
    when(service.readOptional(CurriculumDimSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(incomingCurriculumCreatedEventDF))

    val transformer = new CurriculumDimTransform(sprk, service)
    val sinks = transformer.transform()

    testSinkBySinkName(sinks.flatten, "transformed-curriculum-created", expectedCreatedColsRS, 3)

  }
}
