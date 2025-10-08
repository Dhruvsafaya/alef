package com.alefeducation.warehouse.core

import com.alefeducation.util.Resources.conf
import com.alefeducation.warehouse.models.QueryMeta
import org.apache.log4j.Logger

trait WarehouseTransformer extends Warehouse[QueryMeta] {

  val QUERY_LIMIT = conf.getString("query-limit")

  def tableNotation: Map[String, String] = Map.empty

  def columnNotation: Map[String, String] = Map.empty

  def pkNotation: Map[String, String]

  private[warehouse] def getTable(name: String) = tableNotation.getOrElse(name, name)

  private[warehouse] def getColumn(name: String) = columnNotation.getOrElse(name, name)

  private[warehouse] def getPkColumn(name: String) = pkNotation.getOrElse(name, name)
}
