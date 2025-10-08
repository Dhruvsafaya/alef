package com.alefeducation.warehouse

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FactPracticeTransformerTest extends AnyFunSuite with Matchers {
  test("strippedColumnName should remove prefix") {
    val strippedColumnName = FactPracticeTransformer.getStrippedColumnName("practice", "", "practice_instructional_plan_id")
    strippedColumnName should be ("instructional_plan_id")
  }

  test("strippedColumnName should remove prefix as practice_item if column contains practice_item") {
    val strippedColumnName = FactPracticeTransformer.getStrippedColumnName("practice", "", "practice_item_skill_id")
    strippedColumnName should be ("skill_id")
  }

  test("strippedColumnName should add columnPrefix if column contains skill") {
    val strippedColumnName = FactPracticeTransformer.getStrippedColumnName("practice", "ccl", "practice_item_skill_id")
    strippedColumnName should be ("ccl_skill_id")
  }

  test("makeStagingColumns should return getDate if column contains dw_created_time") {
    val columnName = FactPracticeTransformer.makeStagingColumns("practice", "")("practice_dw_created_time")
    columnName should be ("getdate() as practice_dw_created_time")
  }

  test("makeStagingColumns should return column as is if column does not end with dw_id") {
    val columnName = FactPracticeTransformer.makeStagingColumns("practice", "")("practice_sa_score")
    columnName should be ("practice_sa_score")
  }

  test("makeStagingColumns should return column as is if column contains date") {
    val columnName = FactPracticeTransformer.makeStagingColumns("practice", "")("practice_created_date")
    columnName should be ("practice_created_date")
  }

  test("makeStagingColumns should return column with table prefix if column contains dw_id") {
    val columnName = FactPracticeTransformer.makeStagingColumns("practice", "")("grade_dw_id")
    columnName should be ("dim_grade.grade_dw_id as grade_dw_id")
  }

  test("makeStagingColumns should return column with table practice_item prefix if column contains dw_id and item") {
    val columnName = FactPracticeTransformer.makeStagingColumns("practice", "")("practice_item_lo_dw_id")
    columnName should be ("lo1.lo_dw_id as practice_item_lo_dw_id")
  }

  test("makeStagingColumns should return column with table rel_user prefix if column contains user") {
    val columnName = FactPracticeTransformer.makeStagingColumns("practice", "")("practice_student_dw_id")
    columnName should be ("student1.user_dw_id as practice_student_dw_id")
  }

  test("makeJoin should join statement for column containing dw_id") {
    val joinTuple = FactPracticeTransformer.makeJoin("testalefdw", "staging_practice", "practice", "")("grade_dw_id")
    joinTuple._1 should be ("inner join testalefdw.dim_grade on dim_grade.grade_id = staging_practice.grade_uuid ")
    joinTuple._2 should be ("")
  }

  test("makeJoin should join statement for column containing dw_id and item") {
    val joinTuple = FactPracticeTransformer.makeJoin("testalefdw", "staging_practice", "practice", "")("practice_item_lo_dw_id")
    joinTuple._1 should be ("inner join testalefdw.dim_learning_objective  lo1 on lo1.lo_id = staging_practice.item_lo_uuid ")
    joinTuple._2 should be ("")
  }

  test("makeJoin should join statement for column containing user") {
    val joinTuple = FactPracticeTransformer.makeJoin("testalefdw", "staging_practice", "practice", "")("practice_student_dw_id")
    joinTuple._1 should be ("inner join testalefdw_stage.rel_user  student1 on student1.user_id = staging_practice.student_uuid ")
    joinTuple._2 should be ("")
  }

  test("makeJoin should join statement for column containing dw_id and item and column prefix is ccl") {
    val joinTuple = FactPracticeTransformer.makeJoin("testalefdw", "staging_practice", "practice", "ccl")("practice_item_skill_dw_id")
    joinTuple._1 should be ("inner join testalefdw.dim_ccl_skill  ccl_skill1 on ccl_skill1.ccl_skill_id = staging_practice.item_skill_uuid ")
    joinTuple._2 should be ("")
  }

  test("makeJoin should join statement for column containing class") {
    val joinTuple = FactPracticeTransformer.makeJoin("testalefdw", "staging_practice", "practice", "")("practice_class_dw_id")
    joinTuple._1 should be ("left join (select distinct class_dw_id, class_id from testalefdw.dim_class) dim_class on dim_class.class_id = staging_practice.class_uuid ")
    joinTuple._2 should be ("\n ((nvl(class_uuid, '') = '' and dim_class.class_dw_id isnull) or (nvl(class_uuid, '') <> '' and dim_class.class_dw_id notnull)) ")
  }

  test("makeJoin should join statement for column containing academic year") {
    val joinTuple = FactPracticeTransformer.makeJoin("testalefdw", "staging_practice", "practice", "")("practice_academic_year_dw_id")
    joinTuple._1 should be ("left join testalefdw.dim_academic_year on dim_academic_year.academic_year_id = staging_practice.academic_year_uuid ")
    joinTuple._2 should be ("\n ((nvl(academic_year_uuid, '') = '' and dim_academic_year.academic_year_dw_id isnull) or (nvl(academic_year_uuid, '') <> '' and dim_academic_year.academic_year_dw_id notnull)) ")
  }
}
