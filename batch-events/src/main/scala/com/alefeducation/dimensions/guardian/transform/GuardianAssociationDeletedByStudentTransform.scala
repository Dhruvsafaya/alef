package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationCommonTransform.GuardianAssociationCommonTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationDeletedByStudentTransform.GuardianAssociationDeletedByStudentTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianCommonUtils.getRedshiftGuardians
import com.alefeducation.dimensions.guardian.transform.GuardianRegistrationTransform.GuardianKey
import com.alefeducation.models.GuardianModel.GuardianCommon
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers.{GuardianDimensionCols, GuardianEntity, ParquetGuardianAssociationsSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{lit, size}
import org.apache.spark.sql.{DataFrame, SparkSession}

class GuardianAssociationDeletedByStudentTransform(val session: SparkSession, val service: SparkBatchService) {

  import session.implicits._

  def transform(): Option[DataSink] = {
    val association = service.readOptional(ParquetGuardianAssociationsSource, session)
    val deletedAssociation: Option[DataFrame] = association
      .map(_.transform(_.selectLatestByRowNumber(List("studentId"))).transform(guardianDeletedToCommonSchema))

    val redshiftGuardian = getRedshiftGuardians(session, service)
    val registrationCommon = service.readOptional(GuardianAssociationCommonTransformSink, session)

    val startId = service.getStartIdUpdateStatus(GuardianKey)

    val associationsDeletedByStudent = deletedAssociation.map(
      _.transform(enrichDeletedWithExistingData(_, redshiftGuardian, registrationCommon))
        .genDwId("rel_guardian_dw_id", startId)
    )

    associationsDeletedByStudent.map(df => {
      DataSink(
        GuardianAssociationDeletedByStudentTransformSink,
        df,
        controlTableUpdateOptions = Map(ProductMaxIdType -> GuardianKey)
      )
    })
  }

  private def guardianDeletedToCommonSchema(df: DataFrame): DataFrame = {
    val toBeDeletedStudentsAssociations = df.filter(size($"guardians") === 0).cache
    toBeDeletedStudentsAssociations
      .select($"studentId", $"occurredOn", $"eventType")
      .withColumn("status", lit(4))
  }

  private def enrichDeletedWithExistingData(deleted: DataFrame,
                                            redshiftGuardian: DataFrame,
                                            registrationCommon: Option[DataFrame]): DataFrame = {
    val registrationCommonValue: DataFrame = registrationCommon.getOrElse(Seq.empty[GuardianCommon].toDF())
    val deletedAssociations: DataFrame = redshiftGuardian
      .unionByName(registrationCommonValue)
      .drop("occurredOn", "status")
      .join(deleted.select("studentId", "status", "occurredOn"), "studentId")
      .withColumn("eventType", lit("Deleted"))

    deletedAssociations.transformForSCD(GuardianDimensionCols, GuardianEntity)
  }

}

object GuardianAssociationDeletedByStudentTransform {
  val GuardianAssociationDeletedByStudentTransformService = "transform-guardian-association-deleted-by-student"
  val GuardianAssociationDeletedByStudentTransformSink = "transformed-guardian-association-deleted-by-student-sink"

  val session: SparkSession = SparkSessionUtils.getSession(GuardianAssociationDeletedByStudentTransformService)
  val service = new SparkBatchService(GuardianAssociationDeletedByStudentTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new GuardianAssociationDeletedByStudentTransform(session, service)
    service.run(transformer.transform())
  }

}
