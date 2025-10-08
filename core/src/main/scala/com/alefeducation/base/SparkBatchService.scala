package com.alefeducation.base

import com.alefeducation.bigdata.Sink
import com.alefeducation.io.data.{Delta => DeltaObj}
import com.alefeducation.util.{DateTimeProvider, FeatureService, Resources}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession

class SparkBatchService(override val name: String,
                        override val session: SparkSession,
                        override val dateOffsetColumnPrefix: Option[String] = None)
    extends SparkBatchServiceWriter
    with SparkBatchServiceReader {

  override def prehook: Unit = {
    secretManager.init(name)
    featureService = FeatureService(secretManager)
    val dateTimeProvider = new DateTimeProvider()
    controlTableService.init(DeltaTable.forPath(session, Resources.getMaxIdsTablePath()), dateTimeProvider, DeltaObj)
    dataOffsetService.init(DeltaTable.forPath(session, Resources.getDataOffsetPath()), dateTimeProvider, DeltaObj)
  }

  def run(sink: => Option[Sink]): Unit = {
    runAll(sink.toList)
  }

  def runAll(sink: => List[Sink]): Unit = {
    prehook
    sink.map(save)
    posthook
  }

  override def posthook: Unit = {
    secretManager.close()
  }
}
