package com.alefeducation.warehouse.core

abstract class CommonTransformer extends Transformer {

  override val sql: SqlJdbc = new SqlJdbcImpl()
}
