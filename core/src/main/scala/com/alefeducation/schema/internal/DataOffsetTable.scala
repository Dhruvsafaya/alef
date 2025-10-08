package com.alefeducation.schema.internal

import java.sql.Timestamp

object DataOffsetTable {
  val offsetColumnName = "last_dw_created_time"

  final case class DataOffsetTable(table_name: String, status: String, last_dw_created_time: Option[Timestamp])
}
