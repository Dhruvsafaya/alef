package com.alefeducation.warehouse

import com.alefeducation.warehouse.core.RedshiftTransformer
import scalikejdbc.AutoSession
import com.alefeducation.warehouse.models.{QueryMeta, WarehouseConnection}

object ClassUserTransformer extends RedshiftTransformer {
  override def tableNotation: Map[String, String] = Map.empty

  override def columnNotation: Map[String, String] = Map.empty

  override def pkNotation: Map[String, String] = Map(
    "rel_class_user" -> "rel_class_user_dw_id"
  )

  val pkCol = List(
    "rel_class_user_dw_id"
  )

  val dimColumnNames = List(
    "class_user_created_time",
    "class_user_updated_time",
    "class_user_deleted_time",
    "class_user_dw_created_time",
    "class_user_dw_updated_time",
    "class_user_active_until",
    "class_user_status",
    "rel_class_user_dw_id",
    "class_user_class_dw_id",
    "class_user_user_dw_id",
    "class_user_role_dw_id",
    "class_user_attach_status"
  )

  override def prepareQueries(connection: WarehouseConnection)(implicit session: AutoSession): List[QueryMeta] = {
    val selectPkQuery = getSelectQuery(pkCol, connection)
    val selectQuery = getSelectQuery(dimColumnNames, connection)
    val insertStatement =
      s"""
         |INSERT INTO ${connection.schema}.dim_class_user
         |(
         | ${dimColumnNames.mkString(",\n ")}
         |)
         |(
         | $selectQuery
         |)
         |""".stripMargin
    log.info(s"pkSelectQuery :  $selectPkQuery")
    println(s"insertQuery : $insertStatement")
    List(QueryMeta("rel_class_user", selectPkQuery, insertStatement))
  }

  private def getSelectQuery(cols: List[String], connection: WarehouseConnection): String = {
    s"""SELECT
       | ${makeColumnNames(cols)}
       |FROM ${connection.schema}_stage.rel_class_user
       | JOIN ${connection.schema}_stage.rel_dw_id_mappings ids ON ids.id = rel_class_user.class_uuid AND entity_type = 'class'
       | LEFT JOIN ${connection.schema}_stage.rel_user alias_user ON alias_user.user_id = rel_class_user.user_uuid
       | JOIN ${connection.schema}.dim_role ON dim_role.role_name = rel_class_user.role_uuid
       |WHERE (rel_class_user.user_uuid is null) OR (alias_user.user_id is not null)
       |ORDER BY ${pkCol.mkString(",")}
       |LIMIT $QUERY_LIMIT""".stripMargin
  }

  private def makeColumnNames(cols: List[String]): String = cols.map {
    case "class_user_class_dw_id" => "ids.dw_id"
    case "class_user_user_dw_id" => "alias_user.user_dw_id as user_dw_id"
    case "class_user_role_dw_id" => "dim_role.role_dw_id"
    case col => col
  }.mkString(",\n ")

}
