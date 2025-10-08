package com.alefeducation.dimensions

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.{SparkSuite, SparkTest, TestBaseSpec}
import com.alefeducation.dimensions.question.{QuestionDimensionTransform, QuestionDwIdMappingTransform}
import com.alefeducation.dimensions.question.QuestionDimensionTransform.QuestionMutatedSourceName
import com.alefeducation.dimensions.question.QuestionDwIdMappingTransform.QuestionMutatedDwIdMappingService
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources.getSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class QuestionDimensionSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedTransformColumns = Set(
    "question_id",
    "question_created_time",
    "question_updated_time",
    "question_dw_created_time",
    "question_dw_updated_time",
    "question_status",
    "question_code",
    "question_triggered_by",
    "question_language",
    "question_type",
    "question_max_score",
    "question_version",
    "question_stage",
    "question_variant",
    "question_body",
    "question_validation",
    "question_active_until",
    "question_curriculum_outcomes",
    "question_keywords",
    "question_format_type",
    "question_conditions_of_use",
    "question_resource_type",
    "question_summative_assessment",
    "question_difficulty_level",
    "question_cognitive_dimensions",
    "question_knowledge_dimensions",
    "question_lexile_level",
    "question_lexile_levels",
    "question_copyrights",
    "question_author",
    "question_authored_date",
    "question_skill_id",
    "question_cefr_level",
    "question_proficiency"
  )


  test("transform question created events successfully") {

    val value =
      """
        |{"eventType":"QuestionCreatedEvent","loadtime":"2020-09-06T19:13:19.584+04:00","questionId":"86a1404f-30f9-4793-871a-e2bd994bfe0e","code":"sfhsdfh1","version":"0","type":"MULTIPLE_CHOICE","variant":"AR","language":"AR","body":"{\"prompt\":\"<p>adgsdg</p>\",\"choices\":{\"minChoice\":1,\"maxChoice\":1,\"layoutColumns\":1,\"shuffle\":true,\"listType\":\"NONE\",\"choiceItems\":[{\"feedback\":\"\",\"choiceId\":540,\"weight\":100.0,\"answer\":\"<p>sdg</p>\"},{\"feedback\":\"<p>sdg</p>\",\"choiceId\":0,\"weight\":0.0,\"answer\":\"<p>sadg</p>\"},{\"feedback\":\"\",\"choiceId\":551,\"weight\":0.0,\"answer\":\"<p>sdg</p>\"},{\"feedback\":\"\",\"choiceId\":860,\"weight\":0.0,\"answer\":\"<p>sdg</p>\"}]},\"hints\":[],\"generalFeedback\":\"\",\"correctAnswerFeedback\":\"\",\"wrongAnswerFeedback\":\"\",\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceBody\"}","validation":"{\"validResponse\":{\"choiceIds\":[540],\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidResponse\"},\"scoringType\":\"EXACT_MATCH\",\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidation\"}","stage":"IN_PROGRESS","maxScore":1.0,"metadata":"{\"keywords\":[\"sdg\"],\"resourceType\":\"TEQ1\",\"summativeAssessment\":true,\"difficultyLevel\":\"EASY\",\"cognitiveDimensions\":[\"REMEMBERING\"],\"knowledgeDimensions\":\"FACTUAL\",\"lexileLevel\":\"3\",\"lexileLevels\":[\"3\",\"4\"],\"copyrights\":[],\"conditionsOfUse\":[],\"formatType\":\"QUESTION\",\"author\":\"Rashid\",\"authoredDate\":\"2020-09-06T00:00:00.000\",\"curriculumOutcomes\":[{\"type\":\"sub_standard\",\"id\":\"16284\",\"name\":\"En6.3.L.PA.1\",\"description\":\"Identify intonation patterns when listening.\",\"curriculum\":\"UAE MOE\",\"grade\":\"6\",\"subject\":\"English Access\"}]}","createdBy":"rashid@alefeducation.com","createdAt":1599405198662,"updatedBy":"rashid@alefeducation.com","updatedAt":1599405198662,"occurredOn":"2020-09-06 15:13:18.662","triggeredBy":"9904","eventDateDw":"20200906","eventdate":"2020-09-06"}
        |
        |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(QuestionMutatedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new QuestionDimensionTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.get.output

    assert(df.columns.toSet === expectedTransformColumns)

    assert(df.count() == 1)
    assert[String](df, "question_id", "86a1404f-30f9-4793-871a-e2bd994bfe0e")
    assert[String](df, "question_lexile_level", "3")
    assert[List[String]](df, "question_lexile_levels", List("3", "4"))
  }

  test("transform question created and updated events successfully") {
    val value =
            """
              |[
              |{"eventType":"QuestionCreatedEvent","loadtime":"2020-09-06T19:13:19.584+04:00","questionId":"86a1404f-30f9-4793-871a-e2bd994bfe0e","code":"sfhsdfh1","version":"0","type":"MULTIPLE_CHOICE","variant":"AR","language":"AR","body":"{\"prompt\":\"<p>adgsdg</p>\",\"choices\":{\"minChoice\":1,\"maxChoice\":1,\"layoutColumns\":1,\"shuffle\":true,\"listType\":\"NONE\",\"choiceItems\":[{\"feedback\":\"\",\"choiceId\":540,\"weight\":100.0,\"answer\":\"<p>sdg</p>\"},{\"feedback\":\"<p>sdg</p>\",\"choiceId\":0,\"weight\":0.0,\"answer\":\"<p>sadg</p>\"},{\"feedback\":\"\",\"choiceId\":551,\"weight\":0.0,\"answer\":\"<p>sdg</p>\"},{\"feedback\":\"\",\"choiceId\":860,\"weight\":0.0,\"answer\":\"<p>sdg</p>\"}]},\"hints\":[],\"generalFeedback\":\"\",\"correctAnswerFeedback\":\"\",\"wrongAnswerFeedback\":\"\",\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceBody\"}","validation":"{\"validResponse\":{\"choiceIds\":[540],\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidResponse\"},\"scoringType\":\"EXACT_MATCH\",\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidation\"}","stage":"IN_PROGRESS","maxScore":1.0,"metadata":"{\"keywords\":[\"sdg\"],\"resourceType\":\"TEQ1\",\"summativeAssessment\":true,\"difficultyLevel\":\"EASY\",\"cognitiveDimensions\":[\"REMEMBERING\"],\"knowledgeDimensions\":\"FACTUAL\",\"lexileLevel\":\"3\",\"lexileLevels\":[\"3\",\"4\"],\"copyrights\":[],\"conditionsOfUse\":[],\"formatType\":\"QUESTION\",\"author\":\"Rashid\",\"authoredDate\":\"2020-09-06T00:00:00.000\",\"curriculumOutcomes\":[{\"type\":\"sub_standard\",\"id\":\"16284\",\"name\":\"En6.3.L.PA.1\",\"description\":\"Identify intonation patterns when listening.\",\"curriculum\":\"UAE MOE\",\"grade\":\"6\",\"subject\":\"English Access\"}]}","createdBy":"rashid@alefeducation.com","createdAt":1599405198662,"updatedBy":"rashid@alefeducation.com","updatedAt":1599405198662,"occurredOn":"2020-09-06 15:13:18.662","triggeredBy":"9904","eventDateDw":"20200906","eventdate":"2020-09-06"},
              |{"eventType":"QuestionUpdatedEvent","loadtime":"2020-09-06T19:20:19.584+04:00","questionId":"86a1404f-30f9-4793-871a-e2bd994bfe0e","code":"sfhsdfh2","version":"0","type":"MULTIPLE_CHOICE","variant":"AR","language":"AR","body":"{\"prompt\":\"<p>adgsdg</p>\",\"choices\":{\"minChoice\":1,\"maxChoice\":1,\"layoutColumns\":1,\"shuffle\":true,\"listType\":\"NONE\",\"choiceItems\":[{\"feedback\":\"\",\"choiceId\":540,\"weight\":100.0,\"answer\":\"<p>sdg</p>\"},{\"feedback\":\"<p>sdg</p>\",\"choiceId\":0,\"weight\":0.0,\"answer\":\"<p>sadg</p>\"},{\"feedback\":\"\",\"choiceId\":551,\"weight\":0.0,\"answer\":\"<p>sdg</p>\"},{\"feedback\":\"\",\"choiceId\":860,\"weight\":0.0,\"answer\":\"<p>sdg</p>\"}]},\"hints\":[],\"generalFeedback\":\"\",\"correctAnswerFeedback\":\"\",\"wrongAnswerFeedback\":\"\",\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceBody\"}","validation":"{\"validResponse\":{\"choiceIds\":[540],\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidResponse\"},\"scoringType\":\"EXACT_MATCH\",\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidation\"}","stage":"IN_PROGRESS","maxScore":1.0,"metadata":"{\"keywords\":[\"sdg\"],\"resourceType\":\"TEQ1\",\"summativeAssessment\":true,\"difficultyLevel\":\"EASY\",\"cognitiveDimensions\":[\"REMEMBERING\"],\"knowledgeDimensions\":\"FACTUAL\",\"lexileLevel\":\"3\",\"lexileLevels\":[\"3\",\"4\"],\"copyrights\":[],\"conditionsOfUse\":[],\"formatType\":\"QUESTION\",\"author\":\"Rashid\",\"authoredDate\":\"2020-09-06T00:00:00.000\",\"curriculumOutcomes\":[{\"type\":\"sub_standard\",\"id\":\"16284\",\"name\":\"En6.3.L.PA.1\",\"description\":\"Identify intonation patterns when listening.\",\"curriculum\":\"UAE MOE\",\"grade\":\"6\",\"subject\":\"English Access\"}]}","createdBy":"rashid@alefeducation.com","createdAt":1599405198662,"updatedBy":"rashid@alefeducation.com","updatedAt":1599405198662,"occurredOn":"2020-09-06 15:13:18.662","triggeredBy":"9904","eventDateDw":"20200906","eventdate":"2020-09-06"}
              |]
              |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(QuestionMutatedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new QuestionDimensionTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.get.output

    assert(df.columns.toSet == expectedTransformColumns)
    assert(df.count() == 2)
    assert[String](df, "question_id", "86a1404f-30f9-4793-871a-e2bd994bfe0e")
  }

  test("transform question update events successfully") {
    val value =
            """
              |{"eventType":"QuestionUpdatedEvent","loadtime":"2020-09-06T19:13:19.584+04:00","questionId":"86a1404f-30f9-4793-871a-e2bd994bfe0e","code":"sfhsdfh1","version":"0","type":"MULTIPLE_CHOICE","variant":"AR","language":"AR","body":"{\"prompt\":\"<p>adgsdg</p>\",\"choices\":{\"minChoice\":1,\"maxChoice\":1,\"layoutColumns\":1,\"shuffle\":true,\"listType\":\"NONE\",\"choiceItems\":[{\"feedback\":\"\",\"choiceId\":540,\"weight\":100.0,\"answer\":\"<p>sdg</p>\"},{\"feedback\":\"<p>sdg</p>\",\"choiceId\":0,\"weight\":0.0,\"answer\":\"<p>sadg</p>\"},{\"feedback\":\"\",\"choiceId\":551,\"weight\":0.0,\"answer\":\"<p>sdg</p>\"},{\"feedback\":\"\",\"choiceId\":860,\"weight\":0.0,\"answer\":\"<p>sdg</p>\"}]},\"hints\":[],\"generalFeedback\":\"\",\"correctAnswerFeedback\":\"\",\"wrongAnswerFeedback\":\"\",\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceBody\"}","validation":"{\"validResponse\":{\"choiceIds\":[540],\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidResponse\"},\"scoringType\":\"EXACT_MATCH\",\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidation\"}","stage":"IN_PROGRESS","maxScore":1.0,"metadata":"{\"keywords\":[\"sdg\"],\"resourceType\":\"TEQ1\",\"summativeAssessment\":true,\"difficultyLevel\":\"EASY\",\"cognitiveDimensions\":[\"REMEMBERING\"],\"knowledgeDimensions\":\"FACTUAL\",\"lexileLevel\":\"3\",\"lexileLevels\":[\"3\",\"4\"],\"copyrights\":[],\"conditionsOfUse\":[],\"formatType\":\"QUESTION\",\"author\":\"Rashid\",\"authoredDate\":\"2020-09-06T00:00:00.000\",\"curriculumOutcomes\":[{\"type\":\"sub_standard\",\"id\":\"16284\",\"name\":\"En6.3.L.PA.1\",\"description\":\"Identify intonation patterns when listening.\",\"curriculum\":\"UAE MOE\",\"grade\":\"6\",\"subject\":\"English Access\"}]}","createdBy":"rashid@alefeducation.com","createdAt":1599405198662,"updatedBy":"rashid@alefeducation.com","updatedAt":1599405198662,"occurredOn":"2020-09-06 15:13:18.662","triggeredBy":"9904","eventDateDw":"20200906","eventdate":"2020-09-06"}
              |
              |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(QuestionMutatedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new QuestionDimensionTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.get.output

    assert(df.columns.toSet == expectedTransformColumns)
    assert(df.count() == 1)
    assert[String](df, "question_id", "86a1404f-30f9-4793-871a-e2bd994bfe0e")
    assert[String](df, "question_lexile_level", "3")
    assert[List[String]](df, "question_lexile_levels", List("3", "4"))
  }


  test("should create dwIdMapping sink for questionCreateEvents") {
    val value =
            """
              |{"eventType":"QuestionCreatedEvent","loadtime":"2020-09-06T19:13:19.584+04:00","questionId":"86a1404f-30f9-4793-871a-e2bd994bfe0e","code":"sfhsdfh1","version":"0","type":"MULTIPLE_CHOICE","variant":"AR","language":"AR","body":"{\"prompt\":\"<p>adgsdg</p>\",\"choices\":{\"minChoice\":1,\"maxChoice\":1,\"layoutColumns\":1,\"shuffle\":true,\"listType\":\"NONE\",\"choiceItems\":[{\"feedback\":\"\",\"choiceId\":540,\"weight\":100.0,\"answer\":\"<p>sdg</p>\"},{\"feedback\":\"<p>sdg</p>\",\"choiceId\":0,\"weight\":0.0,\"answer\":\"<p>sadg</p>\"},{\"feedback\":\"\",\"choiceId\":551,\"weight\":0.0,\"answer\":\"<p>sdg</p>\"},{\"feedback\":\"\",\"choiceId\":860,\"weight\":0.0,\"answer\":\"<p>sdg</p>\"}]},\"hints\":[],\"generalFeedback\":\"\",\"correctAnswerFeedback\":\"\",\"wrongAnswerFeedback\":\"\",\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceBody\"}","validation":"{\"validResponse\":{\"choiceIds\":[540],\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidResponse\"},\"scoringType\":\"EXACT_MATCH\",\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidation\"}","stage":"IN_PROGRESS","maxScore":1.0,"metadata":"{\"keywords\":[\"sdg\"],\"resourceType\":\"TEQ1\",\"summativeAssessment\":true,\"difficultyLevel\":\"EASY\",\"cognitiveDimensions\":[\"REMEMBERING\"],\"knowledgeDimensions\":\"FACTUAL\",\"lexileLevel\":\"3\",\"lexileLevels\":[\"3\",\"4\"],\"copyrights\":[],\"conditionsOfUse\":[],\"formatType\":\"QUESTION\",\"author\":\"Rashid\",\"authoredDate\":\"2020-09-06T00:00:00.000\",\"curriculumOutcomes\":[{\"type\":\"sub_standard\",\"id\":\"16284\",\"name\":\"En6.3.L.PA.1\",\"description\":\"Identify intonation patterns when listening.\",\"curriculum\":\"UAE MOE\",\"grade\":\"6\",\"subject\":\"English Access\"}]}","createdBy":"rashid@alefeducation.com","createdAt":1599405198662,"updatedBy":"rashid@alefeducation.com","updatedAt":1599405198662,"occurredOn":"2020-09-06 15:13:18.662","triggeredBy":"9904","eventDateDw":"20200906","eventdate":"2020-09-06"}
              |
              |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(getSource(QuestionMutatedDwIdMappingService).head, sprk)).thenReturn(Some(inputDF))

    val transformer = new QuestionDwIdMappingTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.get.output

    assert(df.columns.toSet == Set("questionId", "entity_created_time", "entity_dw_created_time", "entity_type"))
    assert[String](df, "questionId", "86a1404f-30f9-4793-871a-e2bd994bfe0e")
    assert[String](df, "entity_type", "question")
  }

  test("transform question created events with missing lexileLevels successfully") {
    val value =
            """
              |{"eventType":"QuestionCreatedEvent","loadtime":"2020-09-06T19:13:19.584+04:00","questionId":"86a1404f-30f9-4793-871a-e2bd994bfe0e","code":"sfhsdfh1","version":"0","type":"MULTIPLE_CHOICE","variant":"AR","language":"AR","body":"{\"prompt\":\"<p>adgsdg</p>\",\"choices\":{\"minChoice\":1,\"maxChoice\":1,\"layoutColumns\":1,\"shuffle\":true,\"listType\":\"NONE\",\"choiceItems\":[{\"feedback\":\"\",\"choiceId\":540,\"weight\":100.0,\"answer\":\"<p>sdg</p>\"},{\"feedback\":\"<p>sdg</p>\",\"choiceId\":0,\"weight\":0.0,\"answer\":\"<p>sadg</p>\"},{\"feedback\":\"\",\"choiceId\":551,\"weight\":0.0,\"answer\":\"<p>sdg</p>\"},{\"feedback\":\"\",\"choiceId\":860,\"weight\":0.0,\"answer\":\"<p>sdg</p>\"}]},\"hints\":[],\"generalFeedback\":\"\",\"correctAnswerFeedback\":\"\",\"wrongAnswerFeedback\":\"\",\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceBody\"}","validation":"{\"validResponse\":{\"choiceIds\":[540],\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidResponse\"},\"scoringType\":\"EXACT_MATCH\",\"_class\":\"com.alefeducation.assessmentquestion.model.MultipleChoice$MultipleChoiceValidation\"}","stage":"IN_PROGRESS","maxScore":1.0,"metadata":"{\"keywords\":[\"sdg\"],\"resourceType\":\"TEQ1\",\"summativeAssessment\":true,\"difficultyLevel\":\"EASY\",\"cognitiveDimensions\":[\"REMEMBERING\"],\"knowledgeDimensions\":\"FACTUAL\",\"copyrights\":[],\"conditionsOfUse\":[],\"formatType\":\"QUESTION\",\"author\":\"Rashid\",\"authoredDate\":\"2020-09-06T00:00:00.000\",\"curriculumOutcomes\":[{\"type\":\"sub_standard\",\"id\":\"16284\",\"name\":\"En6.3.L.PA.1\",\"description\":\"Identify intonation patterns when listening.\",\"curriculum\":\"UAE MOE\",\"grade\":\"6\",\"subject\":\"English Access\"}]}","createdBy":"rashid@alefeducation.com","createdAt":1599405198662,"updatedBy":"rashid@alefeducation.com","updatedAt":1599405198662,"occurredOn":"2020-09-06 15:13:18.662","triggeredBy":"9904","eventDateDw":"20200906","eventdate":"2020-09-06"}
              |
              |""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(QuestionMutatedSourceName, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(inputDF))

    val transformer = new QuestionDimensionTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.get.output
    assert(df.columns.toSet == expectedTransformColumns)
    assert(df.count() == 1)
    assert[String](df, "question_id", "86a1404f-30f9-4793-871a-e2bd994bfe0e")
    assert[String](df, "question_lexile_level", null)
    assert[List[String]](df, "question_lexile_levels", null)
  }
}





