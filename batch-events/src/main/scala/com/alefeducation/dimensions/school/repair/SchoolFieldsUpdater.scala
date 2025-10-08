package com.alefeducation.dimensions.school.repair

import com.alefeducation.warehouse.core.WarehouseUpdater
import com.alefeducation.warehouse.models.WarehouseConnection
import scalikejdbc.AutoSession

object DimSchoolRepairJob extends WarehouseUpdater {

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[String] = {
    val schema = connection.schema
    val query =
      s"""
         |update ${schema}.dim_school
         |set school_organization_dw_id       =  o.organization_dw_id
         |from ${schema}.dim_school sc
         |         inner join ${schema}.dim_organization o on sc.school_organization_code = o.organization_code
         |where sc.school_organization_dw_id is null;
         |
         |update ${schema}.dim_school
         |set school_content_repository_dw_id = c.dw_id
         |from ${schema}.dim_school sc
         |         inner join ${schema}_stage.rel_dw_id_mappings c
         |                    on sc.school_content_repository_id = c.id and c.entity_type = 'content-repository';
         |
         |update ${schema}.dim_school
         |set _is_complete = true
         |where dim_school._is_complete = false
         |  and school_organization_dw_id is not null
         |  and school_content_repository_dw_id is not null
         |""".stripMargin

    log.info(query)
    List(query)
  }
}
