package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationCommonTransform.GuardianAssociationCommonTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationTransform.GuardianAssociationTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianCommonUtils.transformGuardianData
import com.alefeducation.dimensions.guardian.transform.GuardianRegistrationTransform.GuardianKey
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession

class GuardianAssociationTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {
    val startId = service.getStartIdUpdateStatus(GuardianKey)

    val associations = service.readOptional(GuardianAssociationCommonTransformSink, session)

    val transformAssociations = associations.map(
      _.transform(transformGuardianData)
        .genDwId("rel_guardian_dw_id", startId)
    )

    transformAssociations.map(df => {
      DataSink(
        GuardianAssociationTransformSink,
        df,
        controlTableUpdateOptions = Map(ProductMaxIdType -> GuardianKey)
      )
    })
  }

}
object GuardianAssociationTransform {
  val GuardianAssociationTransformService = "transform-guardian-association"
  val GuardianAssociationTransformSink = "transformed-guardian-association-sink"

  val session: SparkSession = SparkSessionUtils.getSession(GuardianAssociationTransformService)
  val service = new SparkBatchService(GuardianAssociationTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new GuardianAssociationTransform(session, service)
    service.run(transformer.transform())
  }

}
