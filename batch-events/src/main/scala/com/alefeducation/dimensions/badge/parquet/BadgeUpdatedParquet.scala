package com.alefeducation.dimensions.badge.parquet

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.badge.transform.BadgeUpdatedTransform.BadgeUpdatedParquetSource
import com.alefeducation.util.{ParquetBatchWriter, SparkSessionUtils}
import org.apache.spark.sql.SparkSession

object BadgeUpdatedParquet {
  val BadgeUpdatedParquet = "parquet-badge-updated"

  val session: SparkSession = SparkSessionUtils.getSession(BadgeUpdatedParquetSource)
  val service = new SparkBatchService(BadgeUpdatedParquetSource, session)

  def main(args: Array[String]): Unit = {

    val writer = new ParquetBatchWriter(session, service, BadgeUpdatedParquetSource, BadgeUpdatedParquet)
    service.run(writer.write())
  }
}
