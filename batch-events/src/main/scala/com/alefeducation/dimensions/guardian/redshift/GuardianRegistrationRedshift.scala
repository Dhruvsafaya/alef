package com.alefeducation.dimensions.guardian.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.guardian.transform.GuardianRegistrationTransform.GuardianRegistrationTransformSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.AdminGuardianRegistered
import com.alefeducation.util.DataFrameUtility.setScdTypeIIPostAction
import com.alefeducation.util.Helpers.{GuardianEntity, RedshiftGuardianRelSink}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

object GuardianRegistrationRedshift {

  val serviceName: String = "redshift-guardian-registration"
  val sourceName: String = GuardianRegistrationTransformSink
  val sinkName: String = RedshiftGuardianRelSink

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  def main(args: Array[String]): Unit = {
    val guardianRegistrationTransformed = service.readOptional(sourceName, session)

    val guardianRegistrationRedshiftSink = guardianRegistrationTransformed.map(
      _.toRedshiftSCDSink(sinkName, AdminGuardianRegistered, GuardianEntity, setScdTypeIIPostAction)
    )

    service.run(guardianRegistrationRedshiftSink)
  }
}
