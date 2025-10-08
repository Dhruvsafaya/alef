package com.alefeducation.warehouse.course

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class CourseTransformerTest extends AnyFunSuite with Matchers {

  val transformer = CourseTransformer
  val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""SELECT
         |     rel_course_dw_id
         |FROM testalefdw_stage.rel_course p
         |  JOIN testalefdw.dim_organization o on p.course_organization = o.organization_code and o.organization_status = 1
         |  JOIN testalefdw_stage.rel_dw_id_mappings pdw on p.course_id = pdw.id and pdw.entity_type = 'course'
         |
         |ORDER BY rel_course_dw_id
         |LIMIT 60000""".stripMargin

    queryMetas.head.stagingTable should be("rel_course")
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }
}
