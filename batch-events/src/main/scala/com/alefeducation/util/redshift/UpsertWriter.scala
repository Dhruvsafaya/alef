package com.alefeducation.util.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.service.DataSink
import com.alefeducation.util.DataFrameUtility.getUTCDateFromMillisOrSeconds
import com.alefeducation.util.Resources.{getBool, getNestedMap, getNestedString, getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.redshift.UpsertWriter.getColumnsToUpdate
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode_outer, when}
import org.apache.spark.sql.types.TimestampType

@deprecated
class UpsertWriter(val session: SparkSession,
                   val service: SparkBatchService,
                   val serviceName: String) {
  import com.alefeducation.util.BatchTransformerUtility._

  def write(): Option[DataSink] = {

    val source = getSource(serviceName).head
    val sink = getSink(serviceName).head

    val sourceDf = service.readOptional(source, session)

    val columns = sourceDf.map(_.columns.toList).getOrElse(List.empty)
    val columnsToInsert: Map[String, String] = columns.map { column =>
      column -> s"$TempTableAlias.$column"
    }.toMap

    val columnsToUpdate: Map[String, String] = getColumnsToUpdate(serviceName, columnsToInsert)

    sourceDf.map(
      _.toRedshiftUpsertSink(
        sinkName = sink,
        tableName = getNestedString(serviceName, "table-name"),
        matchConditions = getNestedString(serviceName, "match-conditions"),
        columnsToInsert = columnsToInsert,
        columnsToUpdate = columnsToUpdate,
        isStaging = getBool(serviceName, "is-staging")
      )
    )
  }
}

@deprecated
object UpsertWriter {

  def getColumnsToUpdate(serviceName: String, columnsToInsert: Map[String, String]): Map[String, String] = {
    val entity = getNestedString(serviceName, "entity-prefix")
    if(serviceName.contains("created")) {
      columnsToInsert -
        (s"${entity}_dw_created_time", s"${entity}_deleted_time") +
        (s"${entity}_dw_updated_time" -> s"$TempTableAlias.${entity}_dw_created_time")
    } else if (serviceName.contains("updated")) {
      columnsToInsert -
        (s"${entity}_dw_created_time", s"${entity}_created_time", s"${entity}_deleted_time") +
        (
          s"${entity}_dw_updated_time" -> s"$TempTableAlias.${entity}_dw_created_time",
          s"${entity}_updated_time" -> s"$TempTableAlias.${entity}_created_time"
        )
    } else if (serviceName.contains("deleted")) {
      Map(
        s"${entity}_dw_updated_time" -> s"$TempTableAlias.${entity}_dw_created_time",
        s"${entity}_deleted_time" -> s"$TempTableAlias.${entity}_deleted_time",
        s"${entity}_status" -> s"$TempTableAlias.${entity}_status"
      )
    } else Map.empty
  }

  def main(args: Array[String]): Unit = {

    val serviceName = args(0)

    val session = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val writer = new UpsertWriter(session, service, serviceName)
    service.run(writer.write())
  }
}
