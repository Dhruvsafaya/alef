package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationTransform.GuardianAssociationTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianLatestAssociationTransform.GuardianLatestAssociationTransformSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.DataFrameUtility.selectLatestRecordsByRowNumber
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

class GuardianLatestAssociationTransform(val session: SparkSession, val service: SparkBatchService) {

  import session.implicits._

  def transform(): Option[DataSink] = {
    val guardianAssociation = service.readOptional(GuardianAssociationTransformSink, session)

    val latestAssociations = guardianAssociation.map { associations =>
      selectLatestRecordsByRowNumber(associations, List("guardian_id"), "guardian_created_time")
        .withColumn("guardian_active_until", $"guardian_created_time")
        .withColumn("guardian_status", lit(2))
    }

    latestAssociations.map(df => { DataSink(GuardianLatestAssociationTransformSink, df) })
  }

}

object GuardianLatestAssociationTransform {
  val GuardianLatestAssociationTransformService = "transform-guardian-latest-association"
  val GuardianLatestAssociationTransformSink = "transformed-guardian-latest-association-sink"

  val session: SparkSession = SparkSessionUtils.getSession(GuardianLatestAssociationTransformService)
  val service = new SparkBatchService(GuardianLatestAssociationTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new GuardianLatestAssociationTransform(session, service)
    service.run(transformer.transform())
  }

}
