package com.alefeducation.bigdata

package object batch {

  object Consts {

    val ActiveEnabled = 1
    val Disabled = 3
    val Deleted = 4

  }

  object Action extends Enumeration {
    val CREATE, UPDATE, DISABLED, DELETE = Value
  }

  /**
   * This method is used to generate updateExpr for delta updates.
   * It takes input the colMap of the table and return the list of columns excluding
   * 1. occurredOn
   * 2. eventdate
   * Since both these columns are not present in Redshift schema so
   * @param colMap
   * @return
   */
  def getUpdateColumns(colMap: Map[String, String]): Seq[String] = {
    (colMap.values.toSet -- Set("occurredOn", "eventdate")).toSeq
  }

}
