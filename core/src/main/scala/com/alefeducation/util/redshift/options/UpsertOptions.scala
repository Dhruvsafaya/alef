package com.alefeducation.util.redshift.options

import com.alefeducation.io.data.RedshiftIO.TempTableAlias

object UpsertOptions {
  object RelDwIdMappings {

    def targetTableName: String = "rel_dw_id_mappings"

    def matchConditions(entityType: String): String =
      s"$targetTableName.id = $TempTableAlias.id AND $targetTableName.entity_type='$entityType'"

    def columnsToUpdate: Map[String, String] = Map(
      "entity_dw_created_time" -> s"$TempTableAlias.entity_dw_created_time"
    )

    def columnsToInsert: Map[String, String] = Map(
      "id" -> s"$TempTableAlias.id",
      "entity_type" -> s"$TempTableAlias.entity_type",
      "entity_created_time" -> s"$TempTableAlias.entity_created_time",
      "entity_dw_created_time" -> s"$TempTableAlias.entity_dw_created_time"
    )
  }

  object RelUser {
    def targetTableName: String = "rel_user"

    def matchConditions(entityType: String): String =
      s"$targetTableName.user_id = $TempTableAlias.user_id AND $targetTableName.user_type='$entityType'"

    def columnsToUpdate: Map[String, String] = Map(
      "user_created_time" -> s"$TempTableAlias.user_created_time"
    )

    def columnsToInsert: Map[String, String] = Map(
      "user_id" -> s"$TempTableAlias.user_id",
      "user_type" -> s"$TempTableAlias.user_type",
      "user_created_time" -> s"$TempTableAlias.user_created_time",
      "user_dw_created_time" -> s"$TempTableAlias.user_dw_created_time"
    )
  }

}
