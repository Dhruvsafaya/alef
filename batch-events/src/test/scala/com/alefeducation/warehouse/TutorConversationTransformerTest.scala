package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import com.alefeducation.warehouse.models.WarehouseConnection
import com.alefeducation.warehouse.tutor.TutorConversationTransformer
import scalikejdbc.AutoSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TutorConversationTransformerTest extends AnyFunSuite with Matchers {
  val transformer = TutorConversationTransformer
  val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""SELECT
         |   	ftc_dw_id
         |FROM testalefdw_stage.staging_tutor_conversation tc
         |INNER JOIN testalefdw.dim_grade g ON g.grade_id = tc.ftc_grade_id
         |INNER JOIN testalefdw.dim_tenant t ON t.tenant_id = tc.ftc_tenant_id
         |INNER JOIN testalefdw.dim_school s ON s.school_id = tc.ftc_school_id
         |LEFT JOIN testalefdw.dim_curriculum_subject csu ON csu.curr_subject_id = tc.ftc_subject_id and csu.curr_subject_status=1
         |INNER JOIN testalefdw_stage.rel_user u ON u.user_id = tc.ftc_user_id
         |INNER JOIN testalefdw.dim_learning_objective lo ON lo.lo_id = tc.ftc_activity_id
         |LEFT JOIN testalefdw_stage.rel_dw_id_mappings plc ON plc.id = tc.ftc_level_id and plc.entity_type = 'course_activity_container'
         |LEFT JOIN testalefdw.dim_outcome oc on oc.outcome_id = tc.ftc_outcome_id
         |ORDER BY ftc_dw_id
         |LIMIT 60000""".stripMargin

    queryMetas.head.stagingTable should be("staging_tutor_conversation")
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }

  test("should prepare select statement") {
    val cols = List(
      "ftc_role",
      "ftc_context_id",
      "ftc_school_dw_id",
      "ftc_grade_dw_id",
      "ftc_grade",
      "ftc_subject_dw_id",
      "ftc_subject",
      "ftc_language",
      "ftc_tenant_dw_id",
      "ftc_created_time",
      "ftc_dw_created_time",
      "ftc_date_dw_id",
      "ftc_user_dw_id",
      "ftc_session_id",
      "ftc_activity_dw_id",
      "ftc_activity_status",
      "ftc_material_id",
      "ftc_material_type",
      "ftc_course_activity_container_dw_id",
      "ftc_outcome_dw_id",
      "ftc_conversation_max_tokens",
      "ftc_conversation_token_count",
      "ftc_system_prompt_tokens",
      "ftc_message_language",
      "ftc_message_feedback",
      "ftc_user_message_source",
      "ftc_user_message_tokens",
      "ftc_user_message_timestamp",
      "ftc_bot_message_source",
      "ftc_bot_message_tokens",
      "ftc_bot_message_timestamp",
      "ftc_bot_message_confidence",
      "ftc_bot_message_response_time" ,
      "ftc_bot_suggestions_confidence",
      "ftc_bot_suggestions_response_time",
      "ftc_session_state"
    )
    val expRes = """ftc_role,
                   |	ftc_context_id,
                   |	s.school_dw_id AS ftc_school_dw_id,
                   |	g.grade_dw_id AS ftc_grade_dw_id,
                   |	ftc_grade,
                   |	csu.curr_subject_dw_id AS ftc_subject_dw_id,
                   |	ftc_subject,
                   |	ftc_language,
                   |	t.tenant_dw_id AS ftc_tenant_dw_id,
                   |	ftc_created_time,
                   |	ftc_dw_created_time,
                   |	ftc_date_dw_id,
                   |	u.user_dw_id AS ftc_user_dw_id,
                   |	ftc_session_id,
                   |	lo.lo_dw_id AS ftc_activity_dw_id,
                   |	ftc_activity_status,
                   |	ftc_material_id,
                   |	ftc_material_type,
                   |	plc.dw_id AS ftc_course_activity_container_dw_id,
                   |	oc.outcome_dw_id AS ftc_outcome_dw_id,
                   |	ftc_conversation_max_tokens,
                   |	ftc_conversation_token_count,
                   |	ftc_system_prompt_tokens,
                   |	ftc_message_language,
                   |	ftc_message_feedback,
                   |	ftc_user_message_source,
                   |	ftc_user_message_tokens,
                   |	ftc_user_message_timestamp,
                   |	ftc_bot_message_source,
                   |	ftc_bot_message_tokens,
                   |	ftc_bot_message_timestamp,
                   |	ftc_bot_message_confidence,
                   |	ftc_bot_message_response_time,
                   |	ftc_bot_suggestions_confidence,
                   |	ftc_bot_suggestions_response_time,
                   |    ftc_session_state""".stripMargin

    val actual = transformer.makeColumnNames(cols)
    replaceSpecChars(actual) should be(replaceSpecChars(expRes))
  }
}
