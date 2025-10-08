package com.alefeducation.warehouse.core

case class SqlStringData(insertSql: String => String,
                         startEventsInsert: String => String,
                         idSelectForDelete: String,
                         idSelectForUpdate: String,
                         deleteDanglingSql: String = null)
