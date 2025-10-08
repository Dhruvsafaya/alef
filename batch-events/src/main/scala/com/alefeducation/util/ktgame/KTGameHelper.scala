package com.alefeducation.util.ktgame

import scala.collection.immutable.ListMap

object KTGameHelper {
  val ParquetKTGameSource = "parquet-kt-game-source"
  val ParquetKTGameStartedSource = "parquet-kt-game-session-started-source"
  val ParquetKTGameFinishedSource = "parquet-kt-game-session-finished-source"
  val ParquetKTGameQuestionStartedSource = "parquet-kt-game-question-session-started-source"
  val ParquetKTGameQuestionFinishedSource = "parquet-kt-game-question-session-finished-source"
  val ParquetKTGameSkippedSource = "parquet-kt-game-creation-skipped-source"
  val ParquetTtGameDeltaStagingSource = "parquet-kt-game-delta-staging-source"
  val ParquetTtGameQuestionDeltaStagingSource = "parquet-kt-game-question-delta-staging-source"

  val DeltaKTGame = "delta-kt-game-sink"
  val DeltaKTGameSkippedSource = "delta-kt-game-skipped-sink"

  val ktGameService = "kt-game"
  val ktGameSessionService: String = ktGameService + "-session"
  val RedshiftKTGameSink = "redshift-kt-game-sink"
  val RedshiftKTGameSessionSink = "redshift-kt-game-session-sink"
  val RedshiftKTGameSkippedSink = "redshift-kt-game-skipped-sink"
  val FactKtGame = "ktg"
  val FactKtGameSession: String = FactKtGame + "_session"
  val FactKtGameSkipped: String = FactKtGame + "skipped"

  val DeltaKTGameSessionSink = "delta-kt-game-session-sink"

  val KtGameSessionType = 1
  val KtGameQuestionSessionType = 2

  lazy val StagingKtGame = ListMap(
    "gameSessionId" -> "ktg_id",
    "ktg_date_dw_id" -> "ktg_date_dw_id",
    "tenantId" -> "tenant_uuid",
    "learnerId" -> "student_uuid",
    "subjectId" -> "subject_uuid",
    "schoolId" -> "school_uuid",
    "gradeId" -> "grade_uuid",
    "sectionId" -> "section_uuid",
    "ktGameLoId" -> "lo_uuid",
    "ktGameLoKtCollectionId" -> "ktg_kt_collection_id",
    "ktGameLoNumberOfKeyTerms" -> "ktg_num_key_terms",
    "trimesterId" -> "ktg_trimester_id",
    "trimesterOrder" -> "ktg_trimester_order",
    "gameType" -> "ktg_type",
    "questionType" -> "ktg_question_type",
    "questionMin" -> "ktg_min_question",
    "questionMax" -> "ktg_max_question",
    "questionTime" -> "ktg_question_time_allotted",
    "occurredOn" -> "occurredOn",
    "academicYearId" -> "academic_year_uuid",
    "instructionalPlanId" -> "ktg_instructional_plan_id",
    "learningPathId" -> "ktg_learning_path_id",
    "classId" -> "class_uuid",
    "materialId" -> "ktg_material_id",
    "materialType" -> "ktg_material_type",
  )

  lazy val StagingKtGameSession = ListMap(
    "gameSessionId" -> "ktg_session_id",
    "eventDateDw" -> "ktg_session_date_dw_id",
    "questionId" -> "ktg_session_question_id",
    "keyTermId" -> "ktg_session_kt_id",
    "tenantId" -> "tenant_uuid",
    "learnerId" -> "student_uuid",
    "subjectId" -> "subject_uuid",
    "schoolId" -> "school_uuid",
    "gradeId" -> "grade_uuid",
    "sectionId" -> "section_uuid",
    "ktGameLoId" -> "lo_uuid",
    "outsideOfSchool" -> "ktg_session_outside_of_school",
    "trimesterId" -> "ktg_session_trimester_id",
    "trimesterOrder" -> "ktg_session_trimester_order",
    "gameType" -> "ktg_session_type",
    "questionTime" -> "ktg_session_question_time_allotted",
    "scoreBreakdownAnswer" -> "ktg_session_answer",
    "attempts" -> "ktg_session_num_attempts",
    "score" -> "ktg_session_score",
    "scoreBreakdownMaxScore" -> "ktg_session_max_score",
    "stars" -> "ktg_session_stars",
    "scoreBreakdownIsAttended" -> "ktg_session_is_attended",
    "ktEventType" -> "ktg_session_event_type",
    "isStart" -> "ktg_session_is_start",
    "isStartProcessed" -> "ktg_session_is_start_event_processed",
    "occurredOn" -> "occurredOn",
    "academicYearId" -> "academic_year_uuid",
    "instructionalPlanId" -> "ktg_session_instructional_plan_id",
    "learningPathId" -> "ktg_session_learning_path_id",
    "classId" -> "class_uuid",
    "materialId" -> "ktg_session_material_id",
    "materialType" -> "ktg_session_material_type",
  )

  lazy val StagingKtGameSkipped = ListMap(
    "ktgskipped_date_dw_id" -> "ktgskipped_date_dw_id",
    "tenantId" -> "tenant_uuid",
    "learnerId" -> "student_uuid",
    "subjectId" -> "subject_uuid",
    "schoolId" -> "school_uuid",
    "gradeId" -> "grade_uuid",
    "sectionId" -> "section_uuid",
    "ktGameLoId" -> "lo_uuid",
    "ktGameLoKtCollectionId" -> "ktgskipped_kt_collection_id",
    "ktGameLoNumberOfKeyTerms" -> "ktgskipped_num_key_terms",
    "trimesterId" -> "ktgskipped_trimester_id",
    "trimesterOrder" -> "ktgskipped_trimester_order",
    "gameType" -> "ktgskipped_type",
    "questionType" -> "ktgskipped_question_type",
    "questionMin" -> "ktgskipped_min_question",
    "questionMax" -> "ktgskipped_max_question",
    "questionTime" -> "ktgskipped_question_time_allotted",
    "occurredOn" -> "occurredOn",
    "academicYearId" -> "academic_year_uuid",
    "instructionalPlanId" -> "ktgskipped_instructional_plan_id",
    "learningPathId" -> "ktgskipped_learning_path_id",
    "classId" -> "class_uuid",
    "materialId" -> "ktgskipped_material_id",
    "materialType" -> "ktgskipped_material_type",
  )
}
