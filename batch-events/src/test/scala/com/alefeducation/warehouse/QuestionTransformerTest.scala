package com.alefeducation.warehouse

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.WarehouseConnection

class QuestionTransformerTest extends AnyFunSuite with Matchers {
  test("should prepare queries") {

    val questionTransformer = QuestionTransformer
    val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = questionTransformer.prepareQueries(connection)

    val expectedSelectStatement = """
                                    |select
                                    |     rel_question_id
                                    |from testalefdw_stage.rel_question rc
                                    |inner join testalefdw_stage.rel_dw_id_mappings rdim on rc.question_id = rdim.id
                                    |where rdim.entity_type = 'question'
                                    |order by rel_question_id
                                    |      limit 60000
                                    |""".stripMargin


    queryMetas.head.stagingTable should be ("rel_question")
    queryMetas.head.selectSQL.stripMargin should be (expectedSelectStatement)
  }
}
