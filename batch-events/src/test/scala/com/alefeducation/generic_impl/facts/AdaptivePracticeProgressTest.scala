package com.alefeducation.generic_impl.facts

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.generic_impl.GenericFactTransform
import org.apache.spark.sql.DataFrame
import org.scalatestplus.mockito.MockitoSugar.mock

class AdaptivePracticeProgressTest extends SparkSuite with BaseDimensionSpec {

  val service: SparkBatchService = mock[SparkBatchService]

  test("should transform fact when only AdaptivePracticeSessionAttemptStartedEvent is provided") {

    val startedEvent = """
      |[
      |{
      |  "eventType": "AdaptivePracticeSessionAttemptStartedEvent",
      |  "_trace_id": "b3bb100e-35a1-4ef8-9115-2f56c2854180",
      |  "_app_tenant": "93e4949d-7eff-4707-9201-dac917a5e013",
      |  "uuid": "9d11a768-37b4-4a93-8bf3-5f46d43c7212",
      |  "studentId": "6d4584eb-8d66-491e-9e2f-f08a4ad076b4",
      |  "classId": "3b2680bf-f4f1-4b06-b283-f8c54fe0509b",
      |  "schoolId": "3c4d2d1a-9e66-4aa8-bdbc-16a520fd3450",
      |  "pathwayId": "33bd1e82-2ab2-448e-a1c2-039bb622bb96",
      |  "levelId": "5bac56a6-0c26-43b8-b2cc-18e1e6f1a4a8",
      |  "academicYearTag": "2024-2025",
      |  "sessionId": "8ff238a6-5449-42a6-94e2-5667dfd8e071",
      |  "mlSessionId": "9063f074-b101-402f-b167-aa58e3a6f5a3",
      |  "levelProficiencyScore": 0.75,
      |  "levelProficiencyTier": "PROFICIENT",
      |  "assessmentId": "0a74a826-bfa8-4fa7-a911-420d9b178dcd",
      |  "sessionAttempt": 2,
      |  "stars": 10,
      |  "timeSpent": 24000,
      |  "occurredOn": "2024-03-11T09:27:08.452",
      |  "nextQuestionId": "5bac56a6-0c26-43b8-b2cc-18e1e6f1a4a8",
      |  "nextQuestionSkillId": "52043ed1-91d5-48b5-9c4b-a1263742b4e2",
      |  "nextQuestionDifficultyLabel": "MEDIUM",
      |  "overallProficiencyScore":null,
      |  "overallProficiencyTier": null,
      |  "questionId": null,
      |  "questionSkillId": null,
      |  "answerScore": null,
      |  "questionDifficultyLabel": null,
      |  "timeSpentOnQuestion": null,
      |  "hintUsed": null,
      |  "questionAttemptNumber": null,
      |  "isAnswerCorrect": null,
      |  "skillProficiencyTier": null,
      |  "skillProficiencyScore": null
      |}
      |]
      |""".stripMargin

    val df = genericFactTransformer(Seq(startedEvent))

    assert(df.columns.toSet === expectedColumns)
    val dfRow = df.first()
    assertRow[String](dfRow, "_trace_id", "b3bb100e-35a1-4ef8-9115-2f56c2854180")
    assertRow[String](dfRow, "event_type", "AdaptivePracticeSessionAttemptStartedEvent")
    assertRow[String](dfRow, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assertRow[String](dfRow, "uuid", "9d11a768-37b4-4a93-8bf3-5f46d43c7212")
    assertRow[String](dfRow, "pathway_id", "33bd1e82-2ab2-448e-a1c2-039bb622bb96")
    assertRow[String](dfRow, "level_id", "5bac56a6-0c26-43b8-b2cc-18e1e6f1a4a8")
    assertRow[String](dfRow, "next_question_difficulty_label", "MEDIUM")
  }

  test("should transform fact when AdaptivePracticeAnswerSubmittedEvent is provided") {

    val submittedEvent =
      """[
        |{
        |  "eventType": "AdaptivePracticeAnswerSubmittedEvent",
        |  "_trace_id": "b3bb100e-35a1-4ef8-9115-2f56c2854180",
        |  "_app_tenant": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "uuid": "9d11a768-37b4-4a93-8bf3-5f46d43c7212",
        |  "studentId": "6d4584eb-8d66-491e-9e2f-f08a4ad076b4",
        |  "classId": "3b2680bf-f4f1-4b06-b283-f8c54fe0509b",
        |  "schoolId": "3c4d2d1a-9e66-4aa8-bdbc-16a520fd3450",
        |  "pathwayId": "33bd1e82-2ab2-448e-a1c2-039bb622bb96",
        |  "levelId": "5bac56a6-0c26-43b8-b2cc-18e1e6f1a4a8",
        |  "academicYearTag": "2024-2025",
        |  "sessionId": "8ff238a6-5449-42a6-94e2-5667dfd8e071",
        |  "mlSessionId": "9063f074-b101-402f-b167-aa58e3a6f5a3",
        |  "levelProficiencyScore": 0.75,
        |  "levelProficiencyTier": "PROFICIENT",
        |  "assessmentId": "0a74a826-bfa8-4fa7-a911-420d9b178dcd",
        |  "sessionAttempt": 2,
        |  "stars": 10,
        |  "timeSpent": 24000,
        |  "occurredOn": "2024-03-11T09:27:08.452",
        |  "nextQuestionId": "5bac56a6-0c26-43b8-b2cc-18e1e6f1a4a8",
        |  "nextQuestionSkillId": "52043ed1-91d5-48b5-9c4b-a1263742b4e2",
        |  "nextQuestionDifficultyLabel": "MEDIUM",
        |  "questionId": "6eb1ad70-7b94-4e36-aa9e-e6030dc7e697",
        |  "questionSkillId": "52043ed1-91d5-48b5-9c4b-a1263742b4e2",
        |  "answerScore": 60,
        |  "questionDifficultyLabel": "MEDIUM",
        |  "timeSpentOnQuestion": 10,
        |  "hintUsed": false,
        |  "questionAttemptNumber": 1,
        |  "isAnswerCorrect": true,
        |  "skillProficiencyTier": "PROFICIENT",
        |  "skillProficiencyScore": 0.73,
        |  "overallProficiencyScore":null,
        |  "overallProficiencyTier": null
        |}
        |]
        |""".stripMargin

    val df = genericFactTransformer(Seq(submittedEvent))

    assert(df.columns.toSet === expectedColumns)
    val dfRow = df.first()
    assertRow[String](dfRow, "_trace_id", "b3bb100e-35a1-4ef8-9115-2f56c2854180")
    assertRow[String](dfRow, "event_type", "AdaptivePracticeAnswerSubmittedEvent")
    assertRow[String](dfRow, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assertRow[String](dfRow, "uuid", "9d11a768-37b4-4a93-8bf3-5f46d43c7212")
    assertRow[String](dfRow, "pathway_id", "33bd1e82-2ab2-448e-a1c2-039bb622bb96")
    assertRow[String](dfRow, "level_id", "5bac56a6-0c26-43b8-b2cc-18e1e6f1a4a8")
    assertRow[String](dfRow, "level_proficiency_tier", "PROFICIENT")
    assertRow[Int](dfRow, "time_spent_on_question", 10)
  }

  test("should transform fact when AdaptivePracticeSessionDeletedEvent is provided") {
    val deletedEvent =
      """
          |[
          |{
          |  "eventType": "AdaptivePracticeSessionDeletedEvent",
          |  "_trace_id": "b3bb100f-35a1-4ef8-9115-2f56c2854180",
          |  "_app_tenant": "93e4949d-7eff-4707-9201-dac917a5e013",
          |  "uuid": "9d11a768-37b4-4a93-8bf3-5f46d43c7212",
          |  "studentId": "6d4584eb-8d66-491e-9e2f-f08a4ad076b4",
          |  "classId": "3b2680bf-f4f1-4b06-b283-f8c54fe0509b",
          |  "schoolId": "3c4d2d1a-9e66-4aa8-bdbc-16a520fd3450",
          |  "pathwayId": "33bd1e82-2ab2-448e-a1c2-039bb622bb96",
          |  "levelId": "5bac56a6-0c26-43b8-b2cc-18e1e6f1a4a8",
          |  "academicYearTag": "2024-2025",
          |  "sessionId": "8ff238a6-5449-42a6-94e2-5667dfd8e071",
          |  "mlSessionId": "9063f074-b101-402f-b167-aa58e3a6f5a3",
          |  "overallProficiencyScore": 0.75,
          |  "overallProficiencyTier": "PROFICIENT",
          |  "assessmentId": "0a74a826-bfa8-4fa7-a911-420d9b178dcd",
          |  "sessionAttempt": 2,
          |  "stars": 10,
          |  "timeSpent": 24000,
          |  "occurredOn": "2024-03-11T09:27:08.452",
          |  "nextQuestionId": null,
          |  "nextQuestionSkillId": null,
          |  "nextQuestionDifficultyLabel": null,
          |  "questionId": null,
          |  "questionSkillId": null,
          |  "answerScore": null,
          |  "questionDifficultyLabel": null,
          |  "timeSpentOnQuestion": null,
          |  "hintUsed": null,
          |  "questionAttemptNumber": null,
          |  "isAnswerCorrect": null,
          |  "skillProficiencyTier": null,
          |  "skillProficiencyScore": null,
          |   "levelProficiencyScore": null,
          |  "levelProficiencyTier": null
          |}
          |]
        """.stripMargin

    val df = genericFactTransformer(Seq(deletedEvent))

    assert(df.columns.toSet === expectedColumns)
    val dfRow = df.first()
    assertRow[String](dfRow, "_trace_id", "b3bb100f-35a1-4ef8-9115-2f56c2854180")
    assertRow[String](dfRow, "event_type", "AdaptivePracticeSessionDeletedEvent")
    assertRow[String](dfRow, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assertRow[String](dfRow, "uuid", "9d11a768-37b4-4a93-8bf3-5f46d43c7212")
    assertRow[String](dfRow, "pathway_id", "33bd1e82-2ab2-448e-a1c2-039bb622bb96")
    assertRow[String](dfRow, "level_id", "5bac56a6-0c26-43b8-b2cc-18e1e6f1a4a8")
    assertRow[String](dfRow, "assessment_id", "0a74a826-bfa8-4fa7-a911-420d9b178dcd")
    assertRow[Int](dfRow, "time_spent", 24000)
  }

  def genericFactTransformer(events: Seq[String]): DataFrame = {
    val sprk = spark
    import sprk.implicits._
    val transformer = new GenericFactTransform("adaptive-practice-progress-transformed")
    val inputDf = spark.read.json(events.toDS())

    transformer
      .transform(Map("adaptive_practice_progress" -> Some(inputDf)), 1001)
      .getOrElse(sprk.emptyDataFrame)
  }

  val expectedColumns: Set[String] = Set(
    "event_type",
    "_trace_id",
    "tenant_id",
    "uuid",
    "student_id",
    "class_id",
    "school_id",
    "pathway_id",
    "level_id",
    "academic_year_tag",
    "session_id",
    "ml_session_id",
    "level_proficiency_score",
    "level_proficiency_tier",
    "assessment_id",
    "session_attempt",
    "stars",
    "time_spent",
    "question_id",
    "question_skill_id",
    "question_difficulty_label",
    "skill_proficiency_tier",
    "skill_proficiency_score",
    "answer_score",
    "time_spent_on_question",
    "hint_used",
    "is_answer_correct",
    "next_question_id",
    "attempt_number",
    "next_question_skill_id",
    "next_question_difficulty_label",
    "created_time",
    "date_dw_id",
    "dw_created_time",
    "dw_id",
    "eventdate"
  )

}
