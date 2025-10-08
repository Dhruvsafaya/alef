package com.alefeducation.util.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.{Alias, DeltaUpsertSink}
import com.alefeducation.util.Resources.{getNestedString, getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.delta.UpsertWriter.getColumnsToUpdate
import org.apache.spark.sql.SparkSession

@deprecated
class UpsertWriter(val session: SparkSession,
                   val service: SparkBatchService,
                   val serviceName: String) {

  def write(): Option[DeltaUpsertSink] = {
    import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
    val source = getSource(serviceName).head
    val sink = getSink(serviceName).head

    val sourceDf = service.readOptional(source, session)

    val columns = sourceDf.map(_.columns.toList).getOrElse(List.empty)
    val columnsToInsert: Map[String, String] = columns.map { column =>
      column -> s"${Alias.Events}.$column"
    }.toMap

    val columnsToUpdate: Map[String, String] = getColumnsToUpdate(serviceName, columnsToInsert)

    sourceDf.flatMap(
      _.toUpsert(
        partitionBy = Nil,
        matchConditions = getNestedString(serviceName, "match-conditions"),
        columnsToUpdate = columnsToInsert,
        columnsToInsert = columnsToUpdate
      ).map(_.toSink(sink))
    )
  }
}

object UpsertWriter {

  def getColumnsToUpdate(serviceName: String, columnsToInsert: Map[String, String]): Map[String, String] = {
    val entity = getNestedString(serviceName, "entity-prefix")
    if(serviceName.contains("created")) {
      columnsToInsert
    } else if (serviceName.contains("updated")) {
      columnsToInsert -
        (s"${entity}_created_time", s"${entity}_dw_created_time", s"${entity}_updated_time", s"${entity}_deleted_time", s"${entity}_dw_updated_time") +
        (
          s"${entity}_updated_time" -> s"${Alias.Events}.${entity}_created_time",
          s"${entity}_dw_updated_time" -> s"${Alias.Events}.${entity}_dw_created_time"
        )
    } else if (serviceName.contains("deleted")) {
      Map(
        s"${entity}_dw_updated_time" -> s"${Alias.Events}.${entity}_dw_created_time",
        s"${entity}_deleted_time" -> s"${Alias.Events}.${entity}_deleted_time",
        s"${entity}_status" -> s"${Alias.Events}.${entity}_status"
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
