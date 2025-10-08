package com.alefeducation.util.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{getBool, getList, getString}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class SCDType1Writer(val session: SparkSession,
                     val service: SparkBatchService,
                     val serviceName: String) {
  import com.alefeducation.util.BatchTransformerUtility._

  def write(): Option[DataSink] = {

    val source = getString(serviceName, "source")
    val sink = getString(serviceName, "sink")
    val columnsToBeExcluded = getList(serviceName, "exclude-columns")
    val sourceDf = service.readOptional(source, session).map(_.drop(columnsToBeExcluded:_*))
    val uniqueId = getList(serviceName, "unique-ids")
    val columns = sourceDf.map(_.columns.toList).getOrElse(List.empty)
    val columnsToInsert: Map[String, String] = columns.map { column =>
      column -> s"$TempTableAlias.$column"
    }.toMap

    val tableName = getString(serviceName, "table-name")
    val matchCondition = uniqueId.map(id => s"$tableName.$id = $TempTableAlias.$id").mkString(" and ")

    sourceDf.map(
      _.toRedshiftUpsertSink(
        sinkName = sink,
        tableName = tableName,
        matchConditions = matchCondition,
        columnsToInsert = columnsToInsert,
        columnsToUpdate = columnsToInsert,
        isStaging = getBool(serviceName, "is-staging")
      )
    )
  }
}

object SCDType1Writer {
  def main(args: Array[String]): Unit = {

    val serviceName = args(0)
    val session = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)
    val writer = new SCDType1Writer(session, service, serviceName)
    service.run(writer.write())
  }
}
