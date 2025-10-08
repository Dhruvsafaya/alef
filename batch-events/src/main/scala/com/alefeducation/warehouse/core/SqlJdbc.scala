package com.alefeducation.warehouse.core

import scalikejdbc.{DBSession, WrappedResultSet}

trait SqlJdbc {
  def getIds[A](f: WrappedResultSet => A)(query: String)(implicit session: DBSession): List[A]

  def update(query: String)(implicit session: DBSession): Int

  def localTx(f: DBSession => Unit): Unit
}
