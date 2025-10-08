package com.alefeducation.warehouse

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.WarehouseConnection

class ClassTransformerTest extends AnyFunSuite with Matchers {
  test("should prepare queries") {

    val classTransformer = ClassTransformer
    val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = classTransformer.prepareQueries(connection)

    queryMetas.head.stagingTable should be ("rel_class")
    println(queryMetas.head.insertSQL.replace("\n", ""))
    queryMetas.head.selectSQL should be (expectedSelectStatement)
    queryMetas.head.insertSQL.replace("\n", "") should be (expectedInsertStatement)
  }

  private def expectedSelectStatement: String =     s"""
                                           |select
                                           |     rel_class_dw_id
                                           |from testalefdw_stage.rel_class rc
                                           |inner join testalefdw_stage.rel_dw_id_mappings rdim on rc.class_id=rdim.id
                                           |where rdim.entity_type = 'class'
                                           |order by rel_class_dw_id
                                           |      limit 60000
                                           |""".stripMargin

  private def expectedInsertStatement: String = "INSERT INTO testalefdw.dim_class (rel_class_dw_id, class_dw_id, class_created_time, class_updated_time, class_deleted_time, class_dw_created_time, class_dw_updated_time, class_status, class_id, class_title, class_school_id, class_grade_id, class_section_id, class_academic_year_id, class_academic_calendar_id, class_gen_subject, class_curriculum_id, class_curriculum_grade_id, class_curriculum_subject_id, class_content_academic_year, class_tutor_dhabi_enabled, class_language_direction, class_online, class_practice, class_course_status, class_source_id, class_curriculum_instructional_plan_id, class_category_id, class_active_until, class_material_id, class_material_type) (  select     rel_class_dw_id, dw_id as class_dw_id, class_created_time, class_updated_time, class_deleted_time, class_dw_created_time, class_dw_updated_time, class_status, class_id, class_title, class_school_id, class_grade_id, class_section_id, class_academic_year_id, class_academic_calendar_id, class_gen_subject, class_curriculum_id, class_curriculum_grade_id, class_curriculum_subject_id, class_content_academic_year, class_tutor_dhabi_enabled, class_language_direction, class_online, class_practice, class_course_status, class_source_id, class_curriculum_instructional_plan_id, class_category_id, class_active_until, class_material_id, class_material_typefrom testalefdw_stage.rel_class rcinner join testalefdw_stage.rel_dw_id_mappings rdim on rc.class_id=rdim.idwhere rdim.entity_type = 'class'order by rel_class_dw_id      limit 60000 )"

}
