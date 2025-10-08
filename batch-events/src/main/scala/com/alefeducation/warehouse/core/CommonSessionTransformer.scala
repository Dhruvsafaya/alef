package com.alefeducation.warehouse.core

import com.alefeducation.warehouse.FactSessionTransformer

abstract class CommonSessionTransformer extends FactSessionTransformer {

  override val sql: SqlJdbc = new SqlJdbcImpl()
}
