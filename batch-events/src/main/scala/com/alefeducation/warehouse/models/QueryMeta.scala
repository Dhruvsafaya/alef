package com.alefeducation.warehouse.models

final case class QueryMeta(stagingTable: String, selectSQL: String, insertSQL: String)