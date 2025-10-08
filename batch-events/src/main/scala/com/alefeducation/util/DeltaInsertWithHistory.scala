package com.alefeducation.util

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.util.Constants.ActiveState
import com.alefeducation.util.Resources.{getInt, getNestedString, getSink, getUniqueIds}
import com.alefeducation.util.Utils.{getEntryPrefix, isActiveUntilVersion}
import org.apache.spark.sql.SparkSession

class DeltaInsertWithHistory (val session: SparkSession,
  val service: SparkBatchService,
  val serviceName: String) {
  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  def write(): Option[Sink] = {
    val sourcePath = getNestedString(serviceName, "source")
    val sink = getSink(serviceName).head
    val uniqueIds = getUniqueIds(serviceName)
    val entityPrefix = getEntryPrefix(serviceName, uniqueIds)
    val matchConditions = getNestedString(serviceName, "match-conditions")
    val inactiveStatus = getInt(serviceName, "inactive_status_value")
    val isActiveUntil = isActiveUntilVersion(serviceName)

    val source = service.readOptional(sourcePath, session)
    source.flatMap(
      _.toIWH(
        matchConditions = matchConditions,
        uniqueIdColumns = uniqueIds,
        activeState = ActiveState,
        inactiveStatus = inactiveStatus)
        .map(_.toSink(sink, entityPrefix, isActiveUntilVersion = isActiveUntil))
    )
  }

}
object DeltaInsertWithHistory {
  def main(args: Array[String]): Unit = {

    val serviceName = args(0)

    val session = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val writer = new DeltaInsertWithHistory(session, service, serviceName)
    service.run(writer.write())
  }
}
