package com.alefeducation.warehouse.course

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class CourseContentRepositoryTransformerTest extends AnyFunSuite with Matchers {

  val transformer = CourseContentRepositoryTransformer
  val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""SELECT
         |     ccr_dw_id
         |FROM testalefdw_stage.rel_course_content_repository_association p
         |  LEFT JOIN testalefdw_stage.rel_dw_id_mappings pdw on p.ccr_course_id = pdw.id and pdw.entity_type = 'course'
         |  LEFT JOIN testalefdw.dim_content_repository dcr on p.ccr_repository_id = dcr.content_repository_id
         |WHERE (p.ccr_repository_id is null) or (dcr.content_repository_id is not null)
         |
         |ORDER BY ccr_dw_id
         |LIMIT 60000""".stripMargin

    queryMetas.head.stagingTable should be("rel_course_content_repository_association")
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }
}
