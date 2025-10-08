package com.alefeducation.util.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class InsertIfNotExists(val session: SparkSession,
                        val service: SparkBatchService,
                        val serviceName: String) {

  def write(): Option[DataSink] = {
    val source = getSource(serviceName).head
    val sinkName = getSink(serviceName).head
    val isStaging = getBool(serviceName, "is-staging")
    val tableName = getNestedString(serviceName, "table-name")
    val filterNot = getList(serviceName, "exclude-columns")

    val uniqueIdsStatement = getNestedStringIfPresent(serviceName, "match-conditions")
      .getOrElse(getUniqueIds(serviceName).map(c => s"$c = t.$c").mkString(" AND "))

    val columnCast = getNestedMap(serviceName, "column-cast")

    val sourceDf = service.readOptional(source, session).map(_.drop(filterNot:_*))

    val castedSourceDf = sourceDf.map(doCast(columnCast))

    castedSourceDf.map(_.toInsertIfNotExistsSink(sinkName, tableName, uniqueIdsStatement, isStaging, filterNot))
  }

  def doCast(columnCast: Map[String, String])(df: DataFrame): DataFrame =
    columnCast.foldLeft(df) {
      case (ndf, (name, tp)) => ndf.withColumn(name, col(name).cast(tp))
    }
}

object InsertIfNotExists {
  def main(args: Array[String]): Unit = {

    val serviceName = args(0)

    val session = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val writer = new InsertIfNotExists(session, service, serviceName)
    service.run(writer.write())
  }
}
