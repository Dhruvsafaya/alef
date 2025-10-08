package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class ADTStudentReportFieldsUpdaterTest extends AnyFunSuite with Matchers {
  test("should prepare queries") {

    val adtStudentReportFieldUpdater = ADTStudentReportFieldsUpdater
    val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = adtStudentReportFieldUpdater.prepareQueries(connection)

    println(queryMetas.head.replace("\n", ""))
    queryMetas.head.trim should be (expectedUpdateStatement.trim)
  }

  private def expectedUpdateStatement: String =     s"""
                                                       |   update testalefdw.fact_adt_student_report set fasr_fle_ls_dw_id = fle.fle_dw_id
                                                       |from testalefdw.fact_adt_student_report fasr inner join testalefdw.fact_learning_experience fle on
                                                       |fle.fle_ls_id = fasr.fasr_fle_ls_uuid and fle.fle_exp_ls_flag = false
                                                       |where fasr.fasr_fle_ls_dw_id is null
                                                       |and datediff(days, fle.fle_created_time, fasr.fasr_created_time) <= ${ADTStudentReportFieldsUpdater.DAY_THRESHOLD}""".stripMargin


}
