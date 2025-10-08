package com.alefeducation.warehouse

import com.alefeducation.warehouse.models.WarehouseConnection
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.AutoSession

class ClassUserTransformerTest extends AnyFunSuite with Matchers {
  test("should prepare queries") {

    val classUserTransformer = ClassUserTransformer
    val connection = WarehouseConnection("testalefdw", "http://localhost:8080", "jdbc", "username", "password")
    implicit val autoSession: AutoSession = AutoSession
    val queryMetas = classUserTransformer.prepareQueries(connection)

    queryMetas.head.stagingTable should be ("rel_class_user")
    println(queryMetas.head.insertSQL.replace("\n", ""))
    queryMetas.head.selectSQL should be (expectedSelectStatement)
    queryMetas.head.insertSQL should be (expectedInsertStatement)
  }

  private def expectedSelectStatement: String = """SELECT
                                           | rel_class_user_dw_id
                                           |FROM testalefdw_stage.rel_class_user
                                           | JOIN testalefdw_stage.rel_dw_id_mappings ids ON ids.id = rel_class_user.class_uuid AND entity_type = 'class'
                                           | LEFT JOIN testalefdw_stage.rel_user alias_user ON alias_user.user_id = rel_class_user.user_uuid
                                           | JOIN testalefdw.dim_role ON dim_role.role_name = rel_class_user.role_uuid
                                           |WHERE (rel_class_user.user_uuid is null) OR (alias_user.user_id is not null)
                                           |ORDER BY rel_class_user_dw_id
                                           |LIMIT 60000""".stripMargin

  private def expectedInsertStatement: String =
    """
      |INSERT INTO testalefdw.dim_class_user
      |(
      | class_user_created_time,
      | class_user_updated_time,
      | class_user_deleted_time,
      | class_user_dw_created_time,
      | class_user_dw_updated_time,
      | class_user_active_until,
      | class_user_status,
      | rel_class_user_dw_id,
      | class_user_class_dw_id,
      | class_user_user_dw_id,
      | class_user_role_dw_id,
      | class_user_attach_status
      |)
      |(
      | SELECT
      | class_user_created_time,
      | class_user_updated_time,
      | class_user_deleted_time,
      | class_user_dw_created_time,
      | class_user_dw_updated_time,
      | class_user_active_until,
      | class_user_status,
      | rel_class_user_dw_id,
      | ids.dw_id,
      | alias_user.user_dw_id as user_dw_id,
      | dim_role.role_dw_id,
      | class_user_attach_status
      |FROM testalefdw_stage.rel_class_user
      | JOIN testalefdw_stage.rel_dw_id_mappings ids ON ids.id = rel_class_user.class_uuid AND entity_type = 'class'
      | LEFT JOIN testalefdw_stage.rel_user alias_user ON alias_user.user_id = rel_class_user.user_uuid
      | JOIN testalefdw.dim_role ON dim_role.role_name = rel_class_user.role_uuid
      |WHERE (rel_class_user.user_uuid is null) OR (alias_user.user_id is not null)
      |ORDER BY rel_class_user_dw_id
      |LIMIT 60000
      |)
      |""".stripMargin

}
