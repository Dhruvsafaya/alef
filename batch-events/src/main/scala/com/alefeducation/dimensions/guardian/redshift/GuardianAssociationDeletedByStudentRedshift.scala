package com.alefeducation.dimensions.guardian.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationDeletedByStudentTransform.GuardianAssociationDeletedByStudentTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianCommonUtils.scdUpdateOption
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.AdminGuardianDeleted
import com.alefeducation.util.Helpers.{GuardianEntity, RedshiftGuardianRelSink}
import com.alefeducation.util.{Resources, SparkSessionUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.Map

object GuardianAssociationDeletedByStudentRedshift {

  val serviceName: String = "redshift-guardian-association-deleted-by-student"
  val sourceName: String = GuardianAssociationDeletedByStudentTransformSink
  val sinkName: String = RedshiftGuardianRelSink

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  def main(args: Array[String]): Unit = {
    val associationsDeletedByStudent = service.readOptional(sourceName, session)

    val guardianAssociationDeleteByStudentRedshiftSink = associationsDeletedByStudent.map(
      _.toRedshiftSCDSink(sinkName, AdminGuardianDeleted, GuardianEntity, scdUpdateOption)
    )

    service.run(guardianAssociationDeleteByStudentRedshiftSink)
  }
}
