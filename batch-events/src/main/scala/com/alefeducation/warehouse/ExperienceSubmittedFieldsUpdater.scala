package com.alefeducation.warehouse
import com.alefeducation.warehouse.core.WarehouseUpdater
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.WarehouseConnection

object ExperienceSubmittedFieldsUpdater extends WarehouseUpdater {

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[String] = {
    val schema = connection.schema

    val query =
      s"""
        |update ${schema}.fact_experience_submitted
        |set fes_ls_dw_id  = fle.fle_dw_id,
        |    fes_exp_dw_id = fle.fle_dw_id
        |from ${schema}.fact_experience_submitted fes
        |         inner join (select fle_dw_id, fle_ls_id, fle_exp_id
        |                     from (SELECT t2.fle_dw_id,
        |                                  t2.fle_ls_id,
        |                                  t2.fle_exp_id,
        |                                  t2.fle_created_time,
        |                                  ROW_NUMBER()
        |                                  OVER
        |                                      (PARTITION BY t2.fle_ls_id, t2.fle_exp_id ORDER BY t2.fle_dw_id)
        |                                      AS rnk
        |                           FROM ${schema}.fact_learning_experience t2
        |                           where
        |                               t2.fle_date_dw_id >= TO_CHAR(CURRENT_DATE - INTERVAL '7 DAY'
        |                               , 'YYYYMMDD')
        |                             AND t2.fle_exp_ls_flag = true
        |                          ) exp
        |                     where exp.rnk = 1) fle on fle.fle_exp_id = fes.exp_uuid
        |and fle.fle_ls_id = fes.fes_ls_id where fes.fes_exp_dw_id is null and fes.fes_ls_dw_id is null;
        |""".stripMargin

    log.info(query)
    List(query)
  }
}
