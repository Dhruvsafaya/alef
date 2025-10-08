package com.alefeducation.dimensions.guardian.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.operations.DeltaSCDSink
import com.alefeducation.dimensions.guardian.transform.GuardianCommonUtils.createDeltaSink
import com.alefeducation.dimensions.guardian.transform.GuardianRegistrationTransform.GuardianRegistrationTransformSink
import com.alefeducation.util.Helpers.{DeltaGuardianSink, GuardianEntity}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object GuardianRegistrationDelta {

  val serviceName = "delta-guardian-registration"
  val sourceName: String = GuardianRegistrationTransformSink
  val sinkName: String = DeltaGuardianSink

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  def main(args: Array[String]): Unit = {
    val registrationTransformed: Option[DataFrame] = service.readOptional(sourceName, session)

    val registrationDeltaSink: Option[DeltaSCDSink] = registrationTransformed.map(
      createDeltaSink(_, sinkName, s"${GuardianEntity}_id")
    )

    service.run(registrationDeltaSink)
  }

}
