package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.WarehouseUpdater
import com.alefeducation.warehouse.models.WarehouseConnection
import scalikejdbc.AutoSession

object ADTNextQuestionFieldsUpdater extends WarehouseUpdater {

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[String] = {
    val schema = connection.schema

    val query =
      s"""
         |update $schema.fact_adt_next_question set fanq_fle_ls_dw_id = fle.fle_dw_id
         |from $schema.fact_adt_next_question fanq inner join $schema.fact_learning_experience fle on
         |fle.fle_ls_id = fanq.fanq_fle_ls_uuid and fle.fle_exp_ls_flag = false
         |where fanq.fanq_fle_ls_dw_id is null
         |and datediff(days, fle.fle_created_time, fanq.fanq_created_time) <= $DAY_THRESHOLD
         |""".stripMargin

    log.info(query)
    List(query)
  }
}
