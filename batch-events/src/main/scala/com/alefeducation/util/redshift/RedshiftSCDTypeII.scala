package com.alefeducation.util.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class RedshiftSCDTypeII(val session: SparkSession,
                        val service: SparkBatchService,
                        val serviceName: String) {

  def write(): Option[Sink] = {
    val source = getNestedString(serviceName, "source")
    val sink = getSink(serviceName).head
    val uniqueIds = getUniqueIds(serviceName)
    val dimTableName = getNestedStringIfPresent(serviceName, "dim-table")
    val relTableName = getNestedStringIfPresent(serviceName, "rel-table")
    val entity = getOptionString(serviceName, "entity-prefix").getOrElse("")
    val filterNot = getList(serviceName, "exclude-columns")

    val df = service.readOptional(source, session).map(_.drop(filterNot:_*))

    df.map(_.toRedshiftSCDTypeIISink(
      sink,
      entity,
      uniqueIds,
      dimTableName.get,
      relTableName
    ))
  }
}

object RedshiftSCDTypeII {

  def main(args: Array[String]): Unit = {
    val serviceName = args(0)

    val session = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val writer = new RedshiftSCDTypeII(session, service, serviceName)
    service.run(writer.write())
  }

}