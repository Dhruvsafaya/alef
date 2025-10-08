package com.alefeducation.warehouse.core

import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import scalikejdbc.{AutoSession, ConnectionPool, DBSession, scalikejdbcSQLInterpolationImplicitDef}

class SqlJdbcTest extends AnyFunSuite with Matchers with BeforeAndAfter {

  implicit val session: DBSession = AutoSession

  before {
    Class.forName("org.h2.Driver")
    ConnectionPool.singleton("jdbc:h2:mem:test;MODE=PostgreSQL", "user", "pass")

    sql"""
      create table if not exists dim_tenant (
        tenant_dw_id bigint not null primary key,
        tenant_id varchar(36),
        tenant_name varchar(64)
      )
    """.execute.apply()

    sql"""insert into dim_tenant (tenant_dw_id, tenant_id, tenant_name) values (97, 'd0c5bf82-36fb-45c9-abc6-719283a543ff', 'ind')""".execute.apply()
  }

  after {
    sql"""drop table dim_tenant""".execute.apply()
  }

  val sqlJdbc: SqlJdbc = new SqlJdbcImpl

  test("update should execute insert or update query") {
    val r = sqlJdbc.update("insert into dim_tenant (tenant_dw_id, tenant_id, tenant_name) values (1, '93e4949d-7eff-4707-9201-dac917a5e013', 'moe')")

    r shouldBe 1
  }

  test("getIds should return list of dw ids") {
    val id = sqlJdbc.getIds(_.long("tenant_dw_id"))("select tenant_dw_id from dim_tenant where tenant_name = 'ind'")
    id shouldBe List(97)
  }

  test("localTx should run query in one transaction") {
    sqlJdbc.localTx { implicit session =>
      sqlJdbc.update("insert into dim_tenant (tenant_dw_id, tenant_id, tenant_name) values (3, '2746328b-4109-4434-8517-940d636ffa09', 'uss')")
      sqlJdbc.update("update dim_tenant set tenant_name = 'us' where tenant_dw_id = 3")
    }

    val ids = sqlJdbc.getIds(_.long("tenant_dw_id"))("select tenant_dw_id from dim_tenant where tenant_name = 'us'")
    ids shouldBe List(3)
  }
}
