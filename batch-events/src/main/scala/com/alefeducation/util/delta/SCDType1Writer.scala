package com.alefeducation.util.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.{Alias, DeltaUpsertSink}
import com.alefeducation.util.Resources.{getList, getSink, getSource, getString}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class SCDType1Writer(val session: SparkSession,
                     val service: SparkBatchService,
                     val serviceName: String) {

  def write(): Option[DeltaUpsertSink] = {
    import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
    val source = getString(serviceName, "source")
    val sink = getString(serviceName, "sink")

    val sourceDf = service.readOptional(source, session)
    val columns = sourceDf.map(_.columns.toList).getOrElse(List.empty)
    val columnsToInsert: Map[String, String] = columns.map { column =>
      column -> s"${Alias.Events}.$column"
    }.toMap

    val uniqueId = getList(serviceName, "unique-ids")
    val matchCondition = uniqueId.map(id => s"delta.$id = ${Alias.Events}.$id").mkString(" and ")

    sourceDf.flatMap(
      _.toUpsert(
        partitionBy = Nil,
        matchConditions = matchCondition,
        columnsToUpdate = columnsToInsert,
        columnsToInsert = columnsToInsert
      ).map(_.toSink(sink))
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
