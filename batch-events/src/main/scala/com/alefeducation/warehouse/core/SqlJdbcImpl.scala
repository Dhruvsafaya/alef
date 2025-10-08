package com.alefeducation.warehouse.core

import scalikejdbc.{DB, DBSession, SQL, WrappedResultSet}

class SqlJdbcImpl extends SqlJdbc {

  override def getIds[A](f: WrappedResultSet => A)(query: String)(implicit session: DBSession): List[A] = {
    SQL(query).map(f).list().apply
  }

  override def update(query: String)(implicit session: DBSession): Int = {
    SQL(query).update().apply()
  }

  override def localTx(f: DBSession => Unit): Unit = {
    DB localTx { implicit session =>
      f(session)
    }
  }
}
