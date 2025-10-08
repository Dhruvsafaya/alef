package com.alefeducation.dimensions.curriculum

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.dimensions.curriculum.dim_curriculum_grade.CurriculumGradeDimTransform
import com.alefeducation.dimensions.curriculum.dim_curriculum_grade.CurriculumGradeDimTransform.CurriculumGradeSourceName
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class CurriculumGradeDimensionSpec extends SparkSuite with BaseDimensionSpec {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

val commonGradeColsRS: Seq[String] = Seq(
    "curr_grade_created_time",
    "curr_grade_dw_created_time",
    "curr_grade_updated_time",
    "curr_grade_deleted_time",
    "curr_grade_dw_updated_time"
  )


  val expectedGradeCreatedColsRS: Seq[String] = (CurriculumGradeCreatedDimCols.values.toSeq ++ commonGradeColsRS).diff(Seq("occurredOn"))

  test("Test CurriculumGradeCreatedEvent") {

    val incomingCurriculumGradeCreatedEvent =
      """
        |[
        |{"eventType":"CurriculumGradeCreatedEvent","loadtime":"2020-05-12T07:34:49.498Z","gradeId":123233,"name":"Sam Grade test","occurredOn":"2020-05-12 07:34:49.492","eventDateDw":"20200512"},
        |{"eventType":"CurriculumGradeCreatedEvent","loadtime":"2020-05-12T07:34:57.817Z","gradeId":9273,"name":"Sam Grade test2","occurredOn":"2020-05-12 07:34:57.813","eventDateDw":"20200512"}
        |]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val incomingCurriculumGradeCreatedEventDF = spark.read.json(Seq(incomingCurriculumGradeCreatedEvent).toDS())
    when(service.readOptional(CurriculumGradeSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(incomingCurriculumGradeCreatedEventDF))

    val transformer = new CurriculumGradeDimTransform(sprk, service)
    val sinks = transformer.transform()

    testSinkBySinkName(sinks.flatten, "transformed-curriculum-grade-created", expectedGradeCreatedColsRS, 2)
  }
}
