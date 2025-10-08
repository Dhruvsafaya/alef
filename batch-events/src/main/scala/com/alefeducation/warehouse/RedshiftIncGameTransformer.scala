package com.alefeducation.warehouse

import com.alefeducation.util.Constants.{IncGameCreatedEventType, IncGameUpdatedEventType}
import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object RedshiftIncGameTransformer extends RedshiftTransformer {

  val StagingIncGameTable = "staging_inc_game"
  val StagingIncGameOutcomeTable = "staging_inc_game_outcome"

  def incGameFromStatement(schema: String, statuses: List[Int]): String = {
    val from = s"""
       | FROM ${schema}_stage.$StagingIncGameTable stgincg
       |   INNER JOIN $schema.dim_tenant ON dim_tenant.tenant_id = stgincg.tenant_uuid
       |   INNER JOIN $schema.dim_school ON dim_school.school_id = stgincg.school_uuid
       |   LEFT JOIN $schema.dim_section ON dim_section.section_id = stgincg.section_uuid
       |   INNER JOIN $schema.dim_learning_objective ON dim_learning_objective.lo_id = stgincg.lo_uuid
       |   INNER JOIN ${schema}_stage.rel_user ON rel_user.user_id = stgincg.teacher_uuid
       |   LEFT JOIN $schema.dim_subject ON dim_subject.subject_id = stgincg.subject_uuid
       |   INNER JOIN $schema.dim_grade ON dim_grade.grade_id = stgincg.grade_uuid
       |   LEFT JOIN $schema.dim_class ON dim_class.class_id = stgincg.class_uuid and dim_class.class_status = 1
       |""".stripMargin
    val where =
      s"""| WHERE stgincg.inc_game_event_type IN ${statuses.mkString("(", ",", ")")}
          | AND ((stgincg.section_uuid ISNULL AND $schema.dim_section.section_id ISNULL) OR (stgincg.section_uuid NOTNULL AND $schema.dim_section.section_id NOTNULL))
          | AND ((stgincg.subject_uuid ISNULL AND $schema.dim_subject.subject_id ISNULL) OR (stgincg.subject_uuid NOTNULL AND $schema.dim_subject.subject_id NOTNULL))
          | AND ((stgincg.class_uuid ISNULL AND $schema.dim_class.class_id ISNULL) OR (stgincg.class_uuid NOTNULL AND $schema.dim_class.class_id NOTNULL))
          |""".stripMargin
    val orderLimit =
      s"""| ORDER BY ${getPkColumn(StagingIncGameTable)} LIMIT $QUERY_LIMIT
          |""".stripMargin
    from + where + orderLimit
  }

  def factIncGameInsertQuery(schema: String): String =
    s"""
       |insert into $schema.fact_inc_game
       |( inc_game_id,
       |  inc_game_created_time,
       |  inc_game_dw_created_time,
       |  inc_game_date_dw_id,
       |  inc_game_tenant_dw_id,
       |  inc_game_school_dw_id,
       |  inc_game_section_dw_id,
       |  inc_game_lo_dw_id,
       |  inc_game_title,
       |  inc_game_teacher_dw_id,
       |  inc_game_subject_dw_id,
       |  inc_game_grade_dw_id,
       |  inc_game_num_questions,
       |  inc_game_is_assessment,
       |  inc_game_lesson_component_id,
       |  inc_game_instructional_plan_id,
       |  inc_game_class_dw_id
       |  )
       |      (select stgincg.inc_game_id,
       |             stgincg.inc_game_created_time,
       |             getdate(),
       |             stgincg.inc_game_date_dw_id,
       |             dim_tenant.tenant_dw_id,
       |             dim_school.school_dw_id,
       |             dim_section.section_dw_id,
       |             dim_learning_objective.lo_dw_id,
       |             stgincg.inc_game_title,
       |             rel_user.user_dw_id,
       |             dim_subject.subject_dw_id,
       |             dim_grade.grade_dw_id,
       |             stgincg.inc_game_num_questions,
       |             stgincg.inc_game_is_assessment,
       |             stgincg.inc_game_lesson_component_id,
       |             stgincg.inc_game_instructional_plan_id,
       |             dim_class.class_dw_id
       |      ${incGameFromStatement(schema, List(IncGameCreatedEventType))}
       |)
    """.stripMargin

  def incGamePkForInsertQuery(schema: String): String = " SELECT inc_game_staging_id" + incGameFromStatement(schema, List(1))

  def incGamePkForUpdateQuery(schema: String): String = " SELECT inc_game_staging_id" + incGameFromStatement(schema, List(2))

  def factIncGameUpdateQuery(schema: String): String =
    s"""
       |update $schema.fact_inc_game
       |      set inc_game_title = t.inc_game_title,
       |          inc_game_updated_time = t.inc_game_created_time,
       |          inc_game_dw_updated_time = t.inc_game_dw_updated_time,
       |          inc_game_instructional_plan_id = t.inc_game_instructional_plan_id
       |      from (select stgincg.inc_game_id,
       |                    stgincg.inc_game_created_time,
       |                    getdate() as inc_game_dw_updated_time,
       |                    dim_tenant.tenant_dw_id,
       |                    dim_school.school_dw_id,
       |                    dim_section.section_dw_id,
       |                    dim_learning_objective.lo_dw_id as inc_game_lo_dw_id,
       |                    stgincg.inc_game_title,
       |                    rel_user.user_dw_id as inc_game_teacher_dw_id,
       |                    dim_subject.subject_dw_id as inc_game_subject_dw_id,
       |                    dim_grade.grade_dw_id as inc_game_grade_dw_id,
       |                    stgincg.inc_game_num_questions,
       |                    stgincg.inc_game_is_assessment,
       |                    stgincg.inc_game_lesson_component_id,
       |                    stgincg.inc_game_instructional_plan_id
       |       ${incGameFromStatement(schema, List(IncGameUpdatedEventType))}
       |) t
       |      where fact_inc_game.inc_game_id = t.inc_game_id
    """.stripMargin

  def fromInsertIncGameOutcomeStatement(schema: String): String =
    s"""
       |     from ${schema}_stage.$StagingIncGameOutcomeTable stgincgo
       |              inner join $schema.dim_tenant on dim_tenant.tenant_id = stgincgo.tenant_uuid
       |              inner join ${schema}_stage.rel_user on rel_user.user_id = stgincgo.player_uuid
       |              inner join $schema.dim_learning_objective on dim_learning_objective.lo_id = stgincgo.lo_uuid
       |              order by ${getPkColumn(StagingIncGameOutcomeTable)} limit $QUERY_LIMIT
    """.stripMargin

  def factIncGameOutcomeInsertQuery(schema: String): String =
    s"""
       |insert into $schema.fact_inc_game_outcome
       |(inc_game_outcome_id,
       | inc_game_outcome_created_time,
       | inc_game_outcome_dw_created_time,
       | inc_game_outcome_date_dw_id,
       | inc_game_outcome_tenant_dw_id,
       | inc_game_outcome_session_id,
       | inc_game_outcome_game_id,
       | inc_game_outcome_player_dw_id,
       | inc_game_outcome_lo_dw_id,
       | inc_game_outcome_score,
       | inc_game_outcome_status,
       | inc_game_outcome_is_assessment
       | )
       |    (select stgincgo.inc_game_outcome_id,
       |            stgincgo.inc_game_outcome_created_time,
       |            getdate(),
       |            stgincgo.inc_game_outcome_date_dw_id,
       |            dim_tenant.tenant_dw_id,
       |            stgincgo.session_uuid,
       |            stgincgo.game_uuid,
       |            rel_user.user_dw_id       as inc_game_outcome_player_dw_id,
       |            dim_learning_objective.lo_dw_id as inc_game_outcome_lo_dw_id,
       |            stgincgo.inc_game_outcome_score,
       |            stgincgo.inc_game_outcome_status,
       |            stgincgo.inc_game_outcome_is_assessment
       |    ${fromInsertIncGameOutcomeStatement(schema)}
       |)
    """.stripMargin

  def incGameOutcomePkForInsertQuery(schema: String): String =
    "select inc_game_outcome_staging_id " + fromInsertIncGameOutcomeStatement(schema)

  override def tableNotation: Map[String, String] = Map.empty

  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation: Map[String, String] = Map(
    StagingIncGameTable -> "inc_game_staging_id",
    StagingIncGameOutcomeTable -> "inc_game_outcome_staging_id"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {

    val schema = connection.schema

    val factIncGameInsertSql = factIncGameInsertQuery(schema)
    val factIncGameUpdateSql = factIncGameUpdateQuery(schema)
    val factIncGameOutcomeInsertSql = factIncGameOutcomeInsertQuery(schema)

    val incGamePkForInsertSql = incGamePkForInsertQuery(schema)
    val incGamePkForUpdateSql = incGamePkForUpdateQuery(schema)
    val incGameOutcomePkForInsertSql = incGameOutcomePkForInsertQuery(schema)

    log.info(s"Query Prepared for $StagingIncGameTable is: \n$incGamePkForInsertSql\n$factIncGameInsertSql\n")
    log.info(s"Query Prepared for $StagingIncGameTable is: \n$incGamePkForUpdateSql\n$factIncGameUpdateSql\n")
    log.info(s"Query Prepared for $StagingIncGameOutcomeTable is : $incGameOutcomePkForInsertSql\n$factIncGameOutcomeInsertSql\n")

    List(
      QueryMeta(StagingIncGameTable, incGamePkForInsertSql, factIncGameInsertSql),
      QueryMeta(StagingIncGameTable, incGamePkForUpdateSql, factIncGameUpdateSql),
      QueryMeta(StagingIncGameOutcomeTable, incGameOutcomePkForInsertSql, factIncGameOutcomeInsertSql)
    )
  }
}