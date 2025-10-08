package com.alefeducation.service

import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.internal.ControlTableUtils.UpdateType
import com.alefeducation.util.{AWSSecretManager, FeatureService, Logging, SecretManager}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

case class DataSink(sinkName: String,
                    dataFrame: DataFrame,
                    options: Map[String, String] = Map.empty,
                    override val eventType: String = "",
                    controlTableUpdateOptions: Map[UpdateType, String] = Map.empty
                   ) extends Sink {

  override def name: String = sinkName

  def df: DataFrame = dataFrame

  override def input: DataFrame = dataFrame

  override def controlTableForUpdate(): Map[UpdateType, String] = controlTableUpdateOptions
}

trait Service extends Logging{

  val secretManager: SecretManager = AWSSecretManager

  var featureService: Option[FeatureService] = None

  val name: String

  val session: SparkSession

  def prehook: Unit

  def read(name: String, session: SparkSession, isMandatory: Boolean = true, optionalSchema: Option[StructType] = None): DataFrame

  def transform(): List[Sink]

  def write(sinks: => List[Sink]): Unit

  def run: Unit = {
    // secretManager.init(name)
    // featureService = FeatureService(secretManager)
    prehook
    log.info(s"Running app ${session.sparkContext.appName}")
    log.info("Applying Transformations...")
    write(transform())
    // secretManager.close()
  }
}
