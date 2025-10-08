package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.guardian.transform.GuardianCommonUtils.transformGuardianData
import com.alefeducation.dimensions.guardian.transform.GuardianRegistrationCommonTransform.GuardianRegistrationCommonTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianRegistrationTransform.{GuardianKey, GuardianRegistrationTransformSink}
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class GuardianRegistrationTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {
    val startId = service.getStartIdUpdateStatus(GuardianKey)

    val registeredCommon = service.readOptional(GuardianRegistrationCommonTransformSink, session)

    val registeredTransformed = registeredCommon.map(
      _.transform(transformGuardianData)
        .genDwId("rel_guardian_dw_id", startId)
    )

    registeredTransformed.map(
      df => {
        DataSink(
          GuardianRegistrationTransformSink,
          df,
          controlTableUpdateOptions = Map(ProductMaxIdType -> GuardianKey)
        )
      }
    )
  }
}
object GuardianRegistrationTransform {
  val GuardianKey = "dim_guardian"
  val GuardianRegistrationTransformService = "transform-guardian-registration"
  val GuardianRegistrationTransformSink = "transformed-guardian-registration-sink"

  val session: SparkSession = SparkSessionUtils.getSession(GuardianRegistrationTransformService)
  val service = new SparkBatchService(GuardianRegistrationTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new GuardianRegistrationTransform(session, service)
    service.run(transformer.transform())
  }

}
