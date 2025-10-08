package com.alefeducation.dimensions.curriculum

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.dimensions.curriculum.dim_curriculum_subject.transform.{CurriculumSubjectCreatedTransform, CurriculumSubjectMutatedTransform}
import com.alefeducation.dimensions.curriculum.dim_curriculum_subject.transform.CurriculumSubjectCreatedTransform.CurriculumSubjectCreatedSourceName
import com.alefeducation.dimensions.curriculum.dim_curriculum_subject.transform.CurriculumSubjectMutatedTransform.{CurriculumSubjectDeletedSourceName, CurriculumSubjectUpdatedSourceName}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class CurriculumSubjectDimensionSpec extends SparkSuite with BaseDimensionSpec {

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val commonSubjectCols: Seq[String] = Seq(
    "curr_subject_created_time",
    "curr_subject_dw_created_time",
    "curr_subject_updated_time",
    "curr_subject_deleted_time",
    "curr_subject_dw_updated_time"
  )


  test("Test CurriculumGradeCreatedEvent") {

    val expectedSbjCreatedColsRS: Seq[String] = (CurriculumSbjMutatedDimCols.values.toSeq ++ commonSubjectCols).diff(Seq("occurredOn"))

    val incomingCurriculumGradeCreatedEvent =
      """
        |[
        |{"eventType":"CurriculumSubjectCreatedEvent","loadtime":"2020-05-11T09:55:33.527Z","subjectId":123214,"name":"New test Subject ","skillable":true,"occurredOn":"2020-05-11 09:55:33.524","eventDateDw":"20200511"},
        |{"eventType":"CurriculumSubjectCreatedEvent","loadtime":"2020-05-11T10:02:07.987Z","subjectId":12344,"name":"sdad12","skillable":true,"occurredOn":"2020-05-11 10:02:07.984","eventDateDw":"20200511"},
        |{"eventType":"CurriculumSubjectCreatedEvent","loadtime":"2020-05-12T06:25:43.281Z","subjectId":234,"name":"Sam Test Sbj1","skillable":true,"occurredOn":"2020-05-12 06:25:43.277","eventDateDw":"20200512"}
        |]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val incomingCurriculumSubjectCreatedEventDF = spark.read.json(Seq(incomingCurriculumGradeCreatedEvent).toDS())
    when(service.readOptional(CurriculumSubjectCreatedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(incomingCurriculumSubjectCreatedEventDF))

    val transformer = new CurriculumSubjectCreatedTransform(sprk, service)
    val sinks = transformer.transform()

    testSinkBySinkName(sinks.flatten, "transformed-curriculum-subject-created-source", expectedSbjCreatedColsRS, 3)

  }

  test("Test CurriculumGradeUpdatedEvent & CurriculumSubjectDeletedEvent") {

    val expectedSbjUpdatedColsRS = (CurriculumSbjMutatedDimCols.values.toSeq ++ commonSubjectCols).diff(Seq("occurredOn"))

    val incomingCurriculumGradeUpdatedEvent =
      """
        |[
        |{"eventType":"CurriculumSubjectUpdatedEvent","subjectId":123214,"loadtime":"2020-05-11T09:55:43.866Z","name":"New test Subject  2","skillable":true,"occurredOn":"2020-05-11 09:55:43.862","eventDateDw":"20200511"}
        |]
      """.stripMargin

    val incomingCurriculumGradeDeleteEvent =
      """
        |[
        |{"eventType":"CurriculumSubjectDeletedEvent","loadtime":"2020-05-11T09:55:53.082Z","subjectId":1234,"occurredOn":"2020-05-11 09:55:53.078","eventDateDw":"20200511"},
        |{"eventType":"CurriculumSubjectDeletedEvent","loadtime":"2020-05-11T10:02:29.243Z","subjectId":12344,"occurredOn":"2020-05-11 10:02:29.240","eventDateDw":"20200511"},
        |{"eventType":"CurriculumSubjectDeletedEvent","loadtime":"2020-05-11T10:59:08.288Z","subjectId":123214,"occurredOn":"2020-05-11 10:59:08.279","eventDateDw":"20200511"}
        |]
  """.stripMargin

    val CurriculumSbjDeletedDimCols = Seq(
      "curr_subject_dw_updated_time",
      "curr_subject_dw_created_time",
      "curr_subject_created_time",
      "curr_subject_updated_time",
      "curr_subject_deleted_time",
      "curr_subject_status",
      "curr_subject_id"
    )

    val sprk = spark
    import sprk.implicits._

    val incomingCurriculumSubjectUpdatedEventDF = spark.read.json(Seq(incomingCurriculumGradeUpdatedEvent).toDS())
    when(service.readOptional(CurriculumSubjectUpdatedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(incomingCurriculumSubjectUpdatedEventDF))

    val incomingCurriculumSubjectDeletedEventDF = spark.read.json(Seq(incomingCurriculumGradeDeleteEvent).toDS())
    when(service.readOptional(CurriculumSubjectDeletedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(incomingCurriculumSubjectDeletedEventDF))

    val transformer = new CurriculumSubjectMutatedTransform(sprk, service)
    val sinks = transformer.transform()

    testSinkBySinkName(sinks.flatten, "transformed-curriculum-subject-updated-source", expectedSbjUpdatedColsRS, 1)


    testSinkBySinkName(sinks.flatten, "transformed-curriculum-subject-deleted-source", CurriculumSbjDeletedDimCols, 3)

  }
}
