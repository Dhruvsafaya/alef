package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationCommonTransform.GuardianAssociationCommonTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationDeletedTransform.GuardianAssociationDeletedTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianCommonUtils.getRedshiftGuardians
import com.alefeducation.dimensions.guardian.transform.GuardianRegistrationCommonTransform.GuardianRegistrationCommonTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianRegistrationTransform.GuardianKey
import com.alefeducation.models.GuardianModel.GuardianCommon
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers.{GuardianDimensionCols, GuardianEntity, ParquetGuardianDeletedSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, explode, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

class GuardianAssociationDeletedTransform(val session: SparkSession, val service: SparkBatchService) {

  import session.implicits._

  def transform(): Option[DataSink] = {
    val guardianDeleted = service
      .readOptional(ParquetGuardianDeletedSource, session)
      .map(
        _.select(explode(col("events.uuid")).as("guardianId"), col("occurredOn"))
          .withColumn("status", lit(4))
      )

    val redshiftGuardian = getRedshiftGuardians(session, service)
    val associationCommon = service.readOptional(GuardianAssociationCommonTransformSink, session)
    val registrationCommon = service.readOptional(GuardianRegistrationCommonTransformSink, session)
    val newGuardians = associationCommon.unionOptionalByNameWithEmptyCheck(
      registrationCommon.map(_.select(redshiftGuardian.columns.map(col): _*))
    )

    val startId = service.getStartIdUpdateStatus(GuardianKey)

    val associationsDeletedByEvent = guardianDeleted.map(
      _.transform(enrichDeletedWithExistingData(_, redshiftGuardian, newGuardians))
        .genDwId("rel_guardian_dw_id", startId)
    )

    associationsDeletedByEvent.map(df => {
      DataSink(
        GuardianAssociationDeletedTransformSink,
        df,
        controlTableUpdateOptions = Map(ProductMaxIdType -> GuardianKey)
      )
    })
  }

  private def enrichDeletedWithExistingData(deleted: DataFrame, redshiftGuardian: DataFrame, newGuardians: Option[DataFrame]): DataFrame = {
    val newGuardiansValue: DataFrame = newGuardians.getOrElse(Seq.empty[GuardianCommon].toDF())
    val deletedAssociations: DataFrame = redshiftGuardian
      .unionByName(newGuardiansValue)
      .drop("occurredOn", "status")
      .join(deleted.select("guardianId", "status", "occurredOn"), "guardianId")
      .withColumn("eventType", lit("Deleted"))

    deletedAssociations.transformForSCD(GuardianDimensionCols, GuardianEntity)
  }

}

object GuardianAssociationDeletedTransform {
  val GuardianAssociationDeletedTransformService = "transform-guardian-association-deleted"
  val GuardianAssociationDeletedTransformSink = "transformed-guardian-association-deleted-sink"

  val session: SparkSession = SparkSessionUtils.getSession(GuardianAssociationDeletedTransformService)
  val service = new SparkBatchService(GuardianAssociationDeletedTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new GuardianAssociationDeletedTransform(session, service)
    service.run(transformer.transform())
  }

}
