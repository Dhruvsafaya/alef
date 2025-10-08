package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class ADTNextQuestionFieldsUpdaterTest extends AnyFunSuite with Matchers {

  test("should prepare queries") {
    val expectedUpdateStatement: String =
      s"""
         |update devalefdw.fact_adt_next_question set fanq_fle_ls_dw_id = fle.fle_dw_id
         |from devalefdw.fact_adt_next_question fanq inner join devalefdw.fact_learning_experience fle on
         |fle.fle_ls_id = fanq.fanq_fle_ls_uuid and fle.fle_exp_ls_flag = false
         |where fanq.fanq_fle_ls_dw_id is null
         |and datediff(days, fle.fle_created_time, fanq.fanq_created_time) <= ${ADTNextQuestionFieldsUpdater.DAY_THRESHOLD}
         """.stripMargin

    val adtStudentReportFieldUpdater = ADTNextQuestionFieldsUpdater
    val connection = WarehouseConnection("devalefdw", "http://localhost:8080", "jdbc", "username", "password")
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = adtStudentReportFieldUpdater.prepareQueries(connection)

    println(queryMetas.head.replace("\n", ""))
    queryMetas.head.trim should be (expectedUpdateStatement.trim)
  }
}
