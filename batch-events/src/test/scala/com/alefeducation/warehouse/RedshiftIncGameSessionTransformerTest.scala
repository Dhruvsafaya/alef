package com.alefeducation.warehouse

import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RedshiftIncGameSessionTransformerTest extends AnyFunSuite with Matchers {

  val transformer: RedshiftIncGameSessionTransformer.type = RedshiftIncGameSessionTransformer

  val factCols: String =
    """inc_game_session_id,
      |inc_game_session_start_time,
      |inc_game_session_end_time,
      |inc_game_session_dw_created_time,
      |inc_game_session_date_dw_id,
      |inc_game_session_time_spent,
      |inc_game_session_tenant_dw_id,
      |inc_game_session_game_id,
      |inc_game_session_title,
      |inc_game_session_num_players,
      |inc_game_session_num_joined_players,
      |inc_game_session_started_by_dw_id,
      |inc_game_session_status,
      |inc_game_session_is_start,
      |inc_game_session_is_assessment""".stripMargin

  test("inc game should prepare queries for transform data from staging table to main") {
    val sqlStringData = transformer.prepareSqlStrings("alefdw", factCols)

    transformer.factEntity shouldBe "inc_game"

    replaceSpecChars(sqlStringData.insertSql("1001, 1002, 1003")) shouldBe replaceSpecChars(
      """
        |insert into alefdw.fact_inc_game_session(inc_game_session_id, inc_game_session_start_time, inc_game_session_end_time,
        |                                         inc_game_session_dw_created_time, inc_game_session_date_dw_id,
        |                                         inc_game_session_time_spent, inc_game_session_tenant_dw_id,
        |                                         inc_game_session_game_id, inc_game_session_title, inc_game_session_num_players,
        |                                         inc_game_session_num_joined_players, inc_game_session_started_by_dw_id,
        |                                         inc_game_session_status, inc_game_session_is_start, inc_game_session_is_assessment)
        |select a.inc_game_session_id,
        |       a.inc_game_session_start_time,
        |       a.inc_game_session_end_time,
        |       a.inc_game_session_dw_created_time,
        |       a.inc_game_session_date_dw_id,
        |       a.inc_game_session_time_spent,
        |       a.inc_game_session_tenant_dw_id,
        |       a.inc_game_session_game_id,
        |       a.inc_game_session_title,
        |       a.inc_game_session_num_players,
        |       a.inc_game_session_num_joined_players,
        |       a.inc_game_session_started_by_dw_id,
        |       a.inc_game_session_status,
        |       a.inc_game_session_is_start,
        |       a.inc_game_session_is_assessment
        |from (select *,
        |             case
        |                 when start_status = 1 then DATEDIFF(second, inc_game_session_start_time, inc_game_session_end_time)
        |                 when start_status = 4 then -1 end as inc_game_session_time_spent
        |      from (select st.inc_game_session_staging_id                                                                                        as start_id,
        |                   en.inc_game_session_staging_id                                                                                        as end_id,
        |                   st.inc_game_session_created_time                                                                                      as inc_game_session_start_time,
        |                   en.inc_game_session_created_time                                                                                      as inc_game_session_end_time,
        |                   getdate()                                                                                                             as inc_game_session_dw_created_time,
        |                   en.inc_game_session_date_dw_id                                                                                        as inc_game_session_date_dw_id,
        |                   st.inc_game_session_id                                                                                                as inc_game_session_id,
        |                   dt.tenant_dw_id                                                                                                       as inc_game_session_tenant_dw_id,
        |                   en.game_uuid                                                                                                          as inc_game_session_game_id,
        |                   st.inc_game_session_title                                                                                             as inc_game_session_title,
        |                   st.inc_game_session_num_players                                                                                       as inc_game_session_num_players,
        |                   st.inc_game_session_num_joined_players                                                                                as inc_game_session_num_joined_players,
        |                   en.inc_game_session_is_start                                                                                          as inc_game_session_is_start,
        |                   st.inc_game_session_status                                                                                            as start_status,
        |                   en.inc_game_session_status                                                                                            as inc_game_session_status,
        |                   ru.user_dw_id                                                                                                         as inc_game_session_started_by_dw_id,
        |                   st.inc_game_session_is_assessment                                                                                     as inc_game_session_is_assessment,
        |                   dense_rank()
        |                   over (partition by st.inc_game_session_id, en.inc_game_session_status order by st.inc_game_session_created_time desc) as rnk
        |            from alefdw_stage.staging_inc_game_session st
        |                     join alefdw_stage.staging_inc_game_session en
        |                          on st.inc_game_session_id = en.inc_game_session_id and st.game_uuid = en.game_uuid and
        |                             (st.inc_game_session_status in (1, 4) and en.inc_game_session_status in (2, 3)) and
        |                             st.inc_game_session_is_start = true and en.inc_game_session_is_start = false
        |                     join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |                     join alefdw_stage.rel_user ru on st.inc_game_session_started_by = ru.user_id) game_with_rank
        |      where game_with_rank.rnk = 1) as a
        |where (a.start_id in (1001, 1002, 1003) or a.end_id in (1001, 1002, 1003))
        |order by a.start_id
        |limit 60000;
        |""".stripMargin)

    replaceSpecChars(sqlStringData.startEventsInsert("1001, 1002, 1003")) shouldBe replaceSpecChars(
      """
        |insert into alefdw.fact_inc_game_session(inc_game_session_id, inc_game_session_start_time, inc_game_session_end_time,
        |                                         inc_game_session_dw_created_time, inc_game_session_date_dw_id,
        |                                         inc_game_session_time_spent, inc_game_session_tenant_dw_id,
        |                                         inc_game_session_game_id, inc_game_session_title, inc_game_session_num_players,
        |                                         inc_game_session_num_joined_players, inc_game_session_started_by_dw_id,
        |                                         inc_game_session_status, inc_game_session_is_start, inc_game_session_is_assessment)
        |select a.inc_game_session_id,
        |       a.inc_game_session_start_time,
        |       a.inc_game_session_end_time,
        |       a.inc_game_session_dw_created_time,
        |       a.inc_game_session_date_dw_id,
        |       a.inc_game_session_time_spent,
        |       a.inc_game_session_tenant_dw_id,
        |       a.inc_game_session_game_id,
        |       a.inc_game_session_title,
        |       a.inc_game_session_num_players,
        |       a.inc_game_session_num_joined_players,
        |       a.inc_game_session_started_by_dw_id,
        |       a.inc_game_session_status,
        |       a.inc_game_session_is_start,
        |       a.inc_game_session_is_assessment
        |from (select st.inc_game_session_staging_id         as start_id,
        |             st.inc_game_session_staging_id         as end_id,
        |             st.inc_game_session_created_time       as inc_game_session_start_time,
        |             cast(null as timestamp)                as inc_game_session_end_time,
        |             getdate()                              as inc_game_session_dw_created_time,
        |             st.inc_game_session_date_dw_id         as inc_game_session_date_dw_id,
        |             st.inc_game_session_id                 as inc_game_session_id,
        |             dt.tenant_dw_id                        as inc_game_session_tenant_dw_id,
        |             st.game_uuid                           as inc_game_session_game_id,
        |             st.inc_game_session_title              as inc_game_session_title,
        |             st.inc_game_session_num_players        as inc_game_session_num_players,
        |             st.inc_game_session_num_joined_players as inc_game_session_num_joined_players,
        |             st.inc_game_session_is_start           as inc_game_session_is_start,
        |             st.inc_game_session_status             as inc_game_session_status,
        |             ru.user_dw_id                          as inc_game_session_started_by_dw_id,
        |             cast(null as int)                      as inc_game_session_time_spent,
        |             st.inc_game_session_is_assessment      as inc_game_session_is_assessment
        |      from alefdw_stage.staging_inc_game_session st
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw_stage.rel_user ru on st.inc_game_session_started_by = ru.user_id
        |      where st.inc_game_session_is_start = true
        |        and st.inc_game_session_is_start_event_processed = false) as a
        |where a.start_id in (1001, 1002, 1003)
        |order by a.start_id
        |limit 60000;
        |""".stripMargin)

    replaceSpecChars(sqlStringData.idSelectForUpdate) shouldBe replaceSpecChars(
      """
        |select a.start_id
        |from (select st.inc_game_session_staging_id         as start_id,
        |             st.inc_game_session_staging_id         as end_id,
        |             st.inc_game_session_created_time       as inc_game_session_start_time,
        |             cast(null as timestamp)                as inc_game_session_end_time,
        |             getdate()                              as inc_game_session_dw_created_time,
        |             st.inc_game_session_date_dw_id         as inc_game_session_date_dw_id,
        |             st.inc_game_session_id                 as inc_game_session_id,
        |             dt.tenant_dw_id                        as inc_game_session_tenant_dw_id,
        |             st.game_uuid                           as inc_game_session_game_id,
        |             st.inc_game_session_title              as inc_game_session_title,
        |             st.inc_game_session_num_players        as inc_game_session_num_players,
        |             st.inc_game_session_num_joined_players as inc_game_session_num_joined_players,
        |             st.inc_game_session_is_start           as inc_game_session_is_start,
        |             st.inc_game_session_status             as inc_game_session_status,
        |             ru.user_dw_id                          as inc_game_session_started_by_dw_id,
        |             cast(null as int)                      as inc_game_session_time_spent,
        |             st.inc_game_session_is_assessment      as inc_game_session_is_assessment
        |      from alefdw_stage.staging_inc_game_session st
        |               join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |               join alefdw_stage.rel_user ru on st.inc_game_session_started_by = ru.user_id
        |      where st.inc_game_session_is_start = true
        |        and st.inc_game_session_is_start_event_processed = false) as a
        |order by a.start_id
        |limit 60000;
        |""".stripMargin)

    replaceSpecChars(sqlStringData.idSelectForDelete) shouldBe replaceSpecChars(
      """
        |select a.start_id, a.end_id
        |from (select *,
        |             case
        |                 when start_status = 1 then DATEDIFF(second, inc_game_session_start_time, inc_game_session_end_time)
        |                 when start_status = 4 then -1 end as inc_game_session_time_spent
        |      from (select st.inc_game_session_staging_id                                                                                        as start_id,
        |                   en.inc_game_session_staging_id                                                                                        as end_id,
        |                   st.inc_game_session_created_time                                                                                      as inc_game_session_start_time,
        |                   en.inc_game_session_created_time                                                                                      as inc_game_session_end_time,
        |                   getdate()                                                                                                             as inc_game_session_dw_created_time,
        |                   en.inc_game_session_date_dw_id                                                                                        as inc_game_session_date_dw_id,
        |                   st.inc_game_session_id                                                                                                as inc_game_session_id,
        |                   dt.tenant_dw_id                                                                                                       as inc_game_session_tenant_dw_id,
        |                   en.game_uuid                                                                                                          as inc_game_session_game_id,
        |                   st.inc_game_session_title                                                                                             as inc_game_session_title,
        |                   st.inc_game_session_num_players                                                                                       as inc_game_session_num_players,
        |                   st.inc_game_session_num_joined_players                                                                                as inc_game_session_num_joined_players,
        |                   en.inc_game_session_is_start                                                                                          as inc_game_session_is_start,
        |                   st.inc_game_session_status                                                                                            as start_status,
        |                   en.inc_game_session_status                                                                                            as inc_game_session_status,
        |                   ru.user_dw_id                                                                                                         as inc_game_session_started_by_dw_id,
        |                   st.inc_game_session_is_assessment                                                                                     as inc_game_session_is_assessment,
        |                   dense_rank()
        |                   over (partition by st.inc_game_session_id, en.inc_game_session_status order by st.inc_game_session_created_time desc) as rnk
        |            from alefdw_stage.staging_inc_game_session st
        |                     join alefdw_stage.staging_inc_game_session en
        |                          on st.inc_game_session_id = en.inc_game_session_id and st.game_uuid = en.game_uuid and
        |                             (st.inc_game_session_status in (1, 4) and en.inc_game_session_status in (2, 3)) and
        |                             st.inc_game_session_is_start = true and en.inc_game_session_is_start = false
        |                     join alefdw.dim_tenant dt on st.tenant_uuid = dt.tenant_id
        |                     join alefdw_stage.rel_user ru on st.inc_game_session_started_by = ru.user_id) game_with_rank) as a
        |order by a.start_id
        |limit 60000;
        |""".stripMargin)
  }
}
