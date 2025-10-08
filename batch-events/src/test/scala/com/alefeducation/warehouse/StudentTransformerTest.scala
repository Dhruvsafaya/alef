package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import com.alefeducation.warehouse.pathway.PathwayActivityCompletedTransformer
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class StudentTransformerTest extends AnyFunSuite with Matchers {
  test("should prepare queries") {

    val pathwayActivityCompletedTransformer = StudentTransformer
    val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = pathwayActivityCompletedTransformer.prepareQueries(connection)
    val expectedInsertStatement =
      s"""select
         |	rel_student_id
         |from testalefdw_stage.rel_student rs
         |inner join testalefdw_stage.rel_user u on u.user_id = rs.student_uuid
         |inner join testalefdw.dim_school s on s.school_id = rs.school_uuid
         |inner join testalefdw.dim_grade g on g.grade_id = rs.grade_uuid
         |inner join testalefdw.dim_section sec on sec.section_id = rs.section_uuid
         |
         |order by rel_student_id
         |limit 60000""".stripMargin

    queryMetas.head.stagingTable should be("rel_student")
    queryMetas.head.selectSQL.stripMargin should be(expectedInsertStatement)
  }
}
