package com.alefeducation.warehouse.course

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class CourseSubjectTransformerTest extends AnyFunSuite with Matchers  {
  val transformer = CourseSubjectTransformer
  val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""SELECT
         |     cs_dw_id
         |FROM testalefdw_stage.rel_course_subject_association p
         |  JOIN testalefdw_stage.rel_dw_id_mappings pdw on p.cs_course_id = pdw.id and pdw.entity_type = 'course'
         |  LEFT JOIN testalefdw.dim_curriculum_subject cs on p.cs_subject_id = cs.curr_subject_id
         |ORDER BY cs_dw_id
         |LIMIT 60000""".stripMargin

    queryMetas.head.stagingTable should be("rel_course_subject_association")
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }
}
