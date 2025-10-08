package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class LearningContentSessionTransformerTest extends AnyFunSuite with Matchers {

  private val transformer = LearningContentSessionTransformer
  private val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")

  test("should prepare query") {
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = transformer.prepareQueries(connection)
    val expectedSelectStatement =
      s"""select
         |     fcs_staging_id
         |from testalefdw_stage.staging_content_session cs
         | inner join testalefdw.dim_learning_objective dlo on dlo.lo_id=cs.fcs_lo_id
         | inner join testalefdw_stage.rel_user ru on ru.user_id = cs.fcs_student_id
         | left join testalefdw_stage.rel_dw_id_mappings dm on dm.id = cs.fcs_class_id and dm.entity_type = 'class'
         | inner join testalefdw.dim_grade dg on dg.grade_id = cs.fcs_grade_id
         | inner join testalefdw.dim_tenant dt on dt.tenant_id = cs.fcs_tenant_id
         | inner join testalefdw.dim_school dsc on dsc.school_id = cs.fcs_school_id
         | left join testalefdw.dim_academic_year day on day.academic_year_id=cs.fcs_ay_id
         | left join testalefdw.dim_section dse on dse.section_id = cs.fcs_section_id
         | left join testalefdw.dim_learning_path dlp on dlp.learning_path_id=cs.fcs_lp_id || cs.fcs_school_id || cs.fcs_class_id
         |  and dlp.learning_path_status = 1
         |where
         | (cs.fcs_ay_id IS NULL AND day.academic_year_dw_id IS NULL) OR
         | (cs.fcs_ay_id IS NOT NULL AND day.academic_year_dw_id IS NOT NULL)
         |order by fcs_staging_id
         |limit 60000""".stripMargin

    queryMetas.head.stagingTable should be("staging_content_session")
    queryMetas.head.selectSQL.stripMargin.trim should be(expectedSelectStatement)
  }
}
