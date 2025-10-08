package com.alefeducation.warehouse.course

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class CourseCurriculumTransformerTest extends AnyFunSuite with Matchers {

  val transformer = CourseCurriculumTransformer
  val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""SELECT
         |     cc_dw_id
         |FROM testalefdw_stage.rel_course_curriculum_association p
         |  JOIN testalefdw_stage.rel_dw_id_mappings pdw on p.cc_course_id = pdw.id and pdw.entity_type = 'course'
         |
         |ORDER BY cc_dw_id
         |LIMIT 60000""".stripMargin

    queryMetas.head.stagingTable should be("rel_course_curriculum_association")
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }
}
