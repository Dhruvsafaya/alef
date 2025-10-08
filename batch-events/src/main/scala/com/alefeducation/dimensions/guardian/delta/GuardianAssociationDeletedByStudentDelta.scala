package com.alefeducation.dimensions.guardian.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.operations.DeltaSCDSink
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationDeletedByStudentTransform.GuardianAssociationDeletedByStudentTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianCommonUtils.createDeltaSink
import com.alefeducation.util.Helpers.{DeltaGuardianSink, GuardianEntity}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object GuardianAssociationDeletedByStudentDelta {

  val serviceName = "delta-guardian-association-deleted-by-student"
  val sourceName: String = GuardianAssociationDeletedByStudentTransformSink
  val sinkName: String = DeltaGuardianSink

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  def main(args: Array[String]): Unit = {
    val associationsDeletedByStudent: Option[DataFrame] = service.readOptional(sourceName, session)

    val deletedAssociationByStudentDeltaSink: Option[DeltaSCDSink] = associationsDeletedByStudent.map(
      createDeltaSink(_, sinkName, s"${GuardianEntity}_student_id")
    )

    service.run(deletedAssociationByStudentDeltaSink)
  }
}
