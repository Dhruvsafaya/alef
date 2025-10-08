package com.alefeducation.dimensions.guardian.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.operations.DeltaSCDSink
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationTransform.GuardianAssociationTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianCommonUtils.createDeltaSink
import com.alefeducation.util.Helpers.{DeltaGuardianSink, GuardianEntity}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

object GuardianAssociationDelta {

  val serviceName = "delta-guardian-association"
  val sourceName: String = GuardianAssociationTransformSink
  val sinkName: String = DeltaGuardianSink

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  def main(args: Array[String]): Unit = {
    val transformAssociations = service.readOptional(sourceName, session)

    val guardianAssociationsDeltaSink: Option[DeltaSCDSink] = transformAssociations.map(
      createDeltaSink(_, sinkName, s"${GuardianEntity}_student_id")
    )

    service.run(guardianAssociationsDeltaSink)
  }

}
