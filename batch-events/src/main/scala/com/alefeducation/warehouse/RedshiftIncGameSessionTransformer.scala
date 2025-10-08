package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.{CommonSessionTransformer, SqlStringData}

object RedshiftIncGameSessionTransformer extends CommonSessionTransformer {

  override val factEntity: String = "inc_game"

  override def prepareSqlStrings(schema: String, factCols: String): SqlStringData = {

    val commonIncGameSelfJoins =
      s"""
         |${schema}_stage.staging_inc_game_session st
         |             join ${schema}_stage.staging_inc_game_session en on st.inc_game_session_id = en.inc_game_session_id
         |               and st.game_uuid = en.game_uuid
         |               and (st.inc_game_session_status in (1, 4) and en.inc_game_session_status in (2, 3))
         |               and st.inc_game_session_is_start = true and
         |               en.inc_game_session_is_start = false
        """.stripMargin

    val commonDimJoins =
      s"""
         |   join $schema.dim_tenant dt on st.tenant_uuid = dt.tenant_id
         |   join ${schema}_stage.rel_user ru on st.inc_game_session_started_by = ru.user_id""".stripMargin

    val commonSelect =
      """select
        |       st.inc_game_session_staging_id               as start_id,
        |       en.inc_game_session_staging_id               as end_id,
        |       st.inc_game_session_created_time             as inc_game_session_start_time,
        |       en.inc_game_session_created_time             as inc_game_session_end_time,
        |       getdate()                                    as inc_game_session_dw_created_time,
        |       en.inc_game_session_date_dw_id               as inc_game_session_date_dw_id,
        |       st.inc_game_session_id                       as inc_game_session_id,
        |       dt.tenant_dw_id                              as inc_game_session_tenant_dw_id,
        |       en.game_uuid                                 as inc_game_session_game_id,
        |       st.inc_game_session_title                    as inc_game_session_title,
        |       st.inc_game_session_num_players              as inc_game_session_num_players,
        |       st.inc_game_session_num_joined_players       as inc_game_session_num_joined_players,
        |       en.inc_game_session_is_start                 as inc_game_session_is_start,
        |       st.inc_game_session_status                   as start_status,
        |       en.inc_game_session_status                   as inc_game_session_status,
        |       ru.user_dw_id                                as inc_game_session_started_by_dw_id,
        |       st.inc_game_session_is_assessment            as inc_game_session_is_assessment,
        |       dense_rank() over (partition by st.inc_game_session_id, en.inc_game_session_status order by st.inc_game_session_created_time desc) as rnk
      """.stripMargin

    val commonSelectForStart =
      """select
        |       st.inc_game_session_staging_id               as start_id,
        |       st.inc_game_session_staging_id               as end_id,
        |       st.inc_game_session_created_time             as inc_game_session_start_time,
        |       cast(null as timestamp)                      as inc_game_session_end_time,
        |       getdate()                                    as inc_game_session_dw_created_time,
        |       st.inc_game_session_date_dw_id               as inc_game_session_date_dw_id,
        |       st.inc_game_session_id                       as inc_game_session_id,
        |       dt.tenant_dw_id                              as inc_game_session_tenant_dw_id,
        |       st.game_uuid                                 as inc_game_session_game_id,
        |       st.inc_game_session_title                    as inc_game_session_title,
        |       st.inc_game_session_num_players              as inc_game_session_num_players,
        |       st.inc_game_session_num_joined_players       as inc_game_session_num_joined_players,
        |       st.inc_game_session_is_start                 as inc_game_session_is_start,
        |       st.inc_game_session_status                   as inc_game_session_status,
        |       ru.user_dw_id                                as inc_game_session_started_by_dw_id,
        |       cast(null as int)                            as inc_game_session_time_spent,
        |       st.inc_game_session_is_assessment            as inc_game_session_is_assessment
      """.stripMargin

    val selectIncGameFactCols =
      """select a.inc_game_session_id,
        |        a.inc_game_session_start_time,
        |        a.inc_game_session_end_time,
        |        a.inc_game_session_dw_created_time,
        |        a.inc_game_session_date_dw_id,
        |        a.inc_game_session_time_spent,
        |        a.inc_game_session_tenant_dw_id,
        |        a.inc_game_session_game_id,
        |        a.inc_game_session_title,
        |        a.inc_game_session_num_players,
        |        a.inc_game_session_num_joined_players,
        |        a.inc_game_session_started_by_dw_id,
        |        a.inc_game_session_status,
        |        a.inc_game_session_is_start,
        |        a.inc_game_session_is_assessment
      """.stripMargin

    val commonWhereClause =
      """
        | where st.inc_game_session_is_start = true
        | and st.inc_game_session_is_start_event_processed = false""".stripMargin

    val unionSelect =
      s"""select *,
         |case
         | when start_status  = 1 then DATEDIFF(second, inc_game_session_start_time, inc_game_session_end_time)
         | when start_status = 4 then -1
         | end as inc_game_session_time_spent
         |from ($commonSelect
         |from $commonIncGameSelfJoins
         |   $commonDimJoins) game_with_rank""".stripMargin

    val startUnionSelect =
      s"""$commonSelectForStart
         | from ${schema}_stage.staging_inc_game_session st
         | $commonDimJoins
         | $commonWhereClause""".stripMargin

    val insertColsFact = s"insert into $schema.fact_inc_game_session($factCols)"

    val insertSql = (idClause: String) =>
      s"""
         |$insertColsFact
         |$selectIncGameFactCols
         |from ($unionSelect where game_with_rank.rnk = 1) as a where (a.start_id in ($idClause) or a.end_id in ($idClause)) order by a.start_id limit $QUERY_LIMIT;
      """.stripMargin

    val startEventsInsert = (idClause: String) =>
      s"""
         | $insertColsFact
         | $selectIncGameFactCols from ($startUnionSelect) as a where a.start_id in ($idClause) order by a.start_id limit $QUERY_LIMIT;
      """.stripMargin

    val idSelectForDelete = s"select a.start_id, a.end_id from ($unionSelect) as a order by a.start_id limit $QUERY_LIMIT;"

    val idSelectForUpdate = s"select a.start_id from ($startUnionSelect) as a order by a.start_id limit $QUERY_LIMIT;"

    log.info(s"insert query: ${insertSql("ids")}")
    log.info(s"start events insert query: ${startEventsInsert("ids")}")
    log.info(s"ids select for delete query: $idSelectForDelete")
    log.info(s"ids select for update query: $idSelectForUpdate")

    SqlStringData(insertSql, startEventsInsert, idSelectForDelete, idSelectForUpdate)
  }
}
