package com.alefeducation.dimensions.guardian.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.operations.DeltaSCDSink
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationDeletedTransform.GuardianAssociationDeletedTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianCommonUtils.createDeltaSink
import com.alefeducation.util.Helpers.{DeltaGuardianSink, GuardianEntity}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object GuardianAssociationDeletedDelta {

  val serviceName = "delta-guardian-association-deleted"
  val sourceName: String = GuardianAssociationDeletedTransformSink
  val sinkName: String = DeltaGuardianSink

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  def main(args: Array[String]): Unit = {
    val associationsDeleted: Option[DataFrame] = service.readOptional(sourceName, session)

    val guardianAssociationDeletedDeltaSink: Option[DeltaSCDSink] = associationsDeleted.map(
      createDeltaSink(_, sinkName, s"${GuardianEntity}_id")
    )

    service.run(guardianAssociationDeletedDeltaSink)
  }
}
