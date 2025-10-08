package com.alefeducation.dimensions.guardian.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationTransform.GuardianAssociationTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianCommonUtils.scdUpdateOption
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.AdminGuardianAssociations
import com.alefeducation.util.Helpers.{GuardianEntity, RedshiftGuardianRelSink}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

object GuardianAssociationRedshift {

  val serviceName: String = "redshift-guardian-association"
  val sourceName: String = GuardianAssociationTransformSink
  val sinkName: String = RedshiftGuardianRelSink

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  def main(args: Array[String]): Unit = {
    val transformAssociations = service.readOptional(sourceName, session)

    val guardianAssociationsRedshiftSink = transformAssociations.map(
      _.toRedshiftSCDSink(sinkName, AdminGuardianAssociations, GuardianEntity, scdUpdateOption)
    )

    service.run(guardianAssociationsRedshiftSink)
  }
}
