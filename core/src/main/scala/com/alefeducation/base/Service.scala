package com.alefeducation.base

import com.alefeducation.bigdata.Sink
import com.alefeducation.util.{AWSSecretManager, FeatureService}
import com.launchdarkly.sdk.server.{LDClient, LDConfig}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SparkSession, _}

trait Service {

  val log = Logger.getLogger(classOf[Service])

  val secretManager: AWSSecretManager.type = AWSSecretManager

  val name: String

  val session: SparkSession

  def prehook: Unit

  def posthook: Unit
  
  val controlTableService: ControlTableService = new ControlTableService()

  val dataOffsetService: DataOffsetService = new DataOffsetService()

  var featureService: Option[FeatureService] = None
}

trait ReaderService extends Service {
  def read(name: String,
           session: SparkSession,
           optionalSchema: Option[StructType] = None,
           extraProps: List[(String, String)],
           uniqueColNames: Seq[String] = Nil,
           withoutDropDuplicates: Boolean = false): DataFrame
}

trait WriterService extends Service {
  def save(sink: Sink): Int
}
