package com.alefeducation.dimensions.guardian.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationDeletedTransform.GuardianAssociationDeletedTransformSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.AdminGuardianDeleted
import com.alefeducation.util.DataFrameUtility.setScdTypeIIPostAction
import com.alefeducation.util.Helpers.{GuardianEntity, RedshiftGuardianRelSink}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

object GuardianAssociationDeletedRedshift {
  val serviceName: String = "redshift-guardian-association-deleted"
  val sourceName: String = GuardianAssociationDeletedTransformSink
  val sinkName: String = RedshiftGuardianRelSink

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  def main(args: Array[String]): Unit = {
    val associationsDeletedByEvent = service.readOptional(sourceName, session)

    val guardianAssociationDeleteByEventRedshiftSink = associationsDeletedByEvent.map(
      _.toRedshiftSCDSink(sinkName, AdminGuardianDeleted, GuardianEntity, setScdTypeIIPostAction)
    )

    service.run(guardianAssociationDeleteByEventRedshiftSink)
  }
}
