package com.alefeducation.util

import com.alefeducation.util.Resources.{getBool, getNestedString, getString, hasNestedPath}

object Utils {

  def getEntryPrefix(serviceName: String, uniqueIds: List[String]) = {
    if (hasNestedPath(serviceName, "entity-prefix")) {
      getNestedString(serviceName, "entity-prefix")
    } else {
      uniqueIds.head.split("_").head
    }
  }

  def isActiveUntilVersion(serviceName: String): Boolean = {
    if (hasNestedPath(serviceName, "is-active-until-version"))
      getBool(serviceName, "is-active-until-version")
    else false
  }

  def getDimTable(serviceName: String): Option[String] = {
    if (hasNestedPath(serviceName, "dim-table")) {
      Some(getNestedString(serviceName, "dim-table"))
    } else None
  }
}
