package com.alefeducation.util

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources._
import com.alefeducation.util.Utils.{getEntryPrefix, isActiveUntilVersion}
import org.apache.spark.sql.SparkSession

class RedshiftInsertWithHistory(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  def write(): Option[Sink] = {
    val source = getNestedString(serviceName, "source")
    val sink = getSink(serviceName).head
    val uniqueIds = getUniqueIds(serviceName)
    val inactiveStatusValue = getInt(serviceName, "inactive_status_value")
    val isStagingSink = getBool(serviceName, "is_staging_sink")

    val df = service.readOptional(source, session)

    val entityPrefix = getEntryPrefix(serviceName, uniqueIds)

    val isActiveUntil = isActiveUntilVersion(serviceName)
    val dimTableName = getNestedStringIfPresent(serviceName, "dim-table")
    val relTableName = getNestedStringIfPresent(serviceName, "rel-table")

    val columnsToExclude = getList(serviceName, "excluded-columns")
    val cleanedDf = if (columnsToExclude.isEmpty) {
      df
    } else {
      df.map(_.drop(columnsToExclude: _*))
    }

    cleanedDf.map(
      _.toRedshiftIWHSink(
        sink,
        entityPrefix,
        ids = uniqueIds,
        isStagingSink = isStagingSink,
        inactiveStatus = inactiveStatusValue,
        isActiveUntilVersion = isActiveUntil,
        dimTableName = dimTableName,
        relTableName = relTableName
      )
    )
  }
}

object RedshiftInsertWithHistory {

  def main(args: Array[String]): Unit = {

    val serviceName = args(0)

    val session = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val writer = new RedshiftInsertWithHistory(session, service, serviceName)
    service.run(writer.write())
  }
}
